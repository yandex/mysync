package dcs

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-zookeeper/zk"
	"github.com/yandex/mysync/internal/log"
)

type zkDCS struct {
	logger             *log.Logger
	config             *ZookeeperConfig
	conn               *zk.Conn
	eventsChan         <-chan zk.Event
	disconnectCallback func() error
	isConnected        bool
	connectedChans     []chan struct{}
	connectedLock      sync.Mutex
	closeTimer         *time.Timer
	acl                []zk.ACL
}

type zkLoggerProxy struct{ *log.Logger }

const (
	PathHANodesPrefix      = "ha_nodes"
	PathCascadeNodesPrefix = "cascade_nodes"
)

func (zklp zkLoggerProxy) Printf(fmt string, args ...interface{}) {
	zklp.Debugf(fmt, args...)
}

func retry(config *ZookeeperConfig, operation func() error) error {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = config.BackoffInterval
	b.RandomizationFactor = config.BackoffRandFactor
	b.Multiplier = config.BackoffMultiplier
	b.MaxInterval = config.BackoffMaxInterval
	b.MaxElapsedTime = config.BackoffMaxElapsedTime
	b.Reset()

	return backoff.Retry(operation, backoff.WithMaxRetries(b, config.BackoffMaxRetries))
}

// NewZookeeper returns Zookeeper based DCS storage
func NewZookeeper(config *ZookeeperConfig, logger *log.Logger) (DCS, error) {
	if len(config.Hosts) == 0 {
		return nil, fmt.Errorf("zookeeper not configured, fill zookeeper/hosts in config")
	}
	if config.Namespace == "" {
		return nil, fmt.Errorf("zookeeper not configured, fill zookeeper/namespace in config")
	}
	if !strings.HasPrefix(config.Namespace, sep) {
		return nil, fmt.Errorf("zookeeper namespace should start with /")
	}
	if config.SessionTimeout == 0 {
		return nil, fmt.Errorf("namespace session timeout not configured")
	}

	var conn *zk.Conn
	var ec <-chan zk.Event
	var err error
	var operation func() error
	if config.UseSSL {
		if config.CACert == "" || config.KeyFile == "" || config.CertFile == "" {
			return nil, fmt.Errorf("zookeeper ssl not configured, fill ca_cert/key_file/cert_file in config or disable use_ssl flag")
		}
		tlsConfig, err := CreateTLSConfig(config.CACert, config.CertFile, config.KeyFile)
		if err != nil {
			return nil, err
		}
		baseDialer := net.Dialer{Timeout: config.SessionTimeout}
		dialer, err := GetTLSDialer(config.Hosts, &baseDialer, tlsConfig)
		if err != nil {
			return nil, err
		}

		operation = func() error {
			conn, ec, err = zk.Connect(config.Hosts, config.SessionTimeout, zk.WithLogger(zkLoggerProxy{logger}), zk.WithDialer(dialer))
			return err
		}
	} else {
		operation = func() error {
			conn, ec, err = zk.Connect(config.Hosts, config.SessionTimeout, zk.WithLogger(zkLoggerProxy{logger}))
			return err
		}
	}

	err = retry(config, operation)

	if err != nil {
		return nil, err
	}

	var acl []zk.ACL
	if config.Auth {
		if config.Username == "" || config.Password == "" {
			return nil, fmt.Errorf("zookeeper auth not configured, fill username/password in config or disable auth flag")
		}
		acl = zk.DigestACL(zk.PermAll, config.Username, config.Password)
		err = conn.AddAuth("digest", []byte(fmt.Sprintf("%s:%s", config.Username, config.Password)))
		if err != nil {
			return nil, err
		}
	}
	z := &zkDCS{
		config:             config,
		logger:             logger,
		conn:               conn,
		disconnectCallback: func() error { return nil },
		eventsChan:         ec,
		acl:                acl,
	}
	go z.handleEvents()

	return z, nil
}

func (z *zkDCS) buildFullPath(path string) string {
	return buildFullPath(z.config.Namespace, path)
}

func (z *zkDCS) getSelfLockOwner() LockOwner {
	return LockOwner{z.config.Hostname, os.Getpid()}
}

func (z *zkDCS) makePath(path string) error {
	parts := strings.Split(path, sep)
	prefix := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		prefix = JoinPath(prefix, part)
		_, err := z.retryCreate(prefix, []byte{}, 0, z.acl)
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}
	return nil
}

func (z *zkDCS) handleEvents() {
	for ev := range z.eventsChan {
		z.logger.Debugf("got ZK event %+v", ev)
		if ev.Type == zk.EventSession {
			z.handleSessionEvent(ev)
		}
	}
}

func (z *zkDCS) handleSessionEvent(ev zk.Event) {
	if ev.State == zk.StateHasSession {
		z.connectedLock.Lock()
		if z.closeTimer != nil {
			z.closeTimer.Stop()
			z.closeTimer = nil
		}
		if !z.isConnected {
			defer z.logger.Infof("session established")
			z.isConnected = true
			for _, c := range z.connectedChans {
				close(c)
			}
			z.connectedChans = nil
		}
		z.connectedLock.Unlock()
	} else {
		if z.closeTimer == nil {
			z.closeTimer = time.AfterFunc(z.config.SessionTimeout, func() {
				z.connectedLock.Lock()
				if z.isConnected && z.closeTimer != nil {
					defer z.logger.Info("session lost")
					z.isConnected = false
					err := z.disconnectCallback()
					if err != nil {
						z.logger.Errorf("Disconnect callback failure: %s", err.Error())
					}
				}
				z.connectedLock.Unlock()
			})
		}
	}
}

func (z *zkDCS) SetDisconnectCallback(callback func() error) {
	z.connectedLock.Lock()
	defer z.connectedLock.Unlock()
	z.disconnectCallback = callback
}

func (z *zkDCS) IsConnected() bool {
	z.connectedLock.Lock()
	defer z.connectedLock.Unlock()
	return z.isConnected
}

func (z *zkDCS) WaitConnected(timeout time.Duration) bool {
	z.connectedLock.Lock()
	if z.isConnected {
		z.connectedLock.Unlock()
		return true
	}
	c := make(chan struct{})
	z.connectedChans = append(z.connectedChans, c)
	z.connectedLock.Unlock()
	t := time.NewTimer(timeout)
	select {
	case <-c:
		return true
	case <-t.C:
		z.logger.Errorf("failed to connect to DCS within %s", timeout)
		return false
	}
}

func (z *zkDCS) Initialize() {
	err := z.makePath(z.config.Namespace)
	if err != nil {
		z.logger.Errorf("failed create root path %s : %v", z.config.Namespace, err)
	}
}

func (z *zkDCS) retryRequestInternal(code func() error) error {
	err := code()
	if err != zk.ErrConnectionClosed {
		return nil
	}
	if !z.IsConnected() {
		return nil
	}
	return err
}

func (z *zkDCS) retryRequest(code func() error) {
	operation := func() error {
		return z.retryRequestInternal(code)
	}

	err := retry(z.config, operation)

	if err != nil {
		z.logger.Errorf("retryRequest failed: %v", err)
	}
}

//nolint:unparam
func (z *zkDCS) retryChildren(path string) (children []string, stat *zk.Stat, err error) {
	z.retryRequest(func() error {
		children, stat, err = z.conn.Children(path)
		return err
	})
	return
}

func (z *zkDCS) retryGet(path string) (data []byte, stat *zk.Stat, err error) {
	z.retryRequest(func() error {
		data, stat, err = z.conn.Get(path)
		return err
	})
	return
}

func (z *zkDCS) retrySet(path string, data []byte, version int32) (stat *zk.Stat, err error) {
	z.retryRequest(func() error {
		stat, err = z.conn.Set(path, data, version)
		return err
	})
	return
}

// nolint: unparam
func (z *zkDCS) retryCreate(path string, data []byte, flags int32, acl []zk.ACL) (rpath string, err error) {
	if acl == nil {
		acl = zk.WorldACL(zk.PermAll)
	}
	z.retryRequest(func() error {
		rpath, err = z.conn.Create(path, data, flags, acl)
		return err
	})
	return
}

func (z *zkDCS) retryDelete(path string, version int32) (err error) {
	z.retryRequest(func() error {
		err = z.conn.Delete(path, version)
		return err
	})
	return
}

func (z *zkDCS) AcquireLock(path string) bool {
	fullPath := z.buildFullPath(path)
	self := z.getSelfLockOwner()
	data, _, err := z.retryGet(fullPath)
	if err != nil && err != zk.ErrNoNode {
		z.logger.Errorf("failed to get lock info %s: %v", fullPath, err)
		return false
	}
	if err == zk.ErrNoNode {
		data, err = json.Marshal(&self)
		if err != nil {
			panic(fmt.Sprintf("failed to serialize to JSON %#v", self))
		}
		_, err = z.retryCreate(fullPath, data, zk.FlagEphemeral, nil)
		if err != nil {
			if err != zk.ErrNodeExists {
				z.logger.Errorf("failed to acquire lock %s: %v", fullPath, err)
			}
			return false
		}
		return true
	}
	owner := LockOwner{}
	if err = json.Unmarshal(data, &owner); err != nil {
		z.logger.Errorf("malformed lock data %s (%s): %v", fullPath, data, err)
		return false
	}
	return owner == self
}

func (z *zkDCS) ReleaseLock(path string) {
	fullPath := z.buildFullPath(path)
	data, stat, err := z.retryGet(fullPath)
	if err != nil && err != zk.ErrNoNode {
		z.logger.Errorf("failed to get lock info %s: %v", fullPath, err)
		return
	}
	owner := LockOwner{}
	if err = json.Unmarshal(data, &owner); err != nil {
		z.logger.Errorf("unexpected lock data %s (%s): %v", fullPath, data, err)
		return
	}
	if owner != z.getSelfLockOwner() {
		z.logger.Errorf("failed to release lock %s: process is not an owner", fullPath)
		return
	}
	err = z.retryDelete(fullPath, stat.Version)
	if err != nil {
		z.logger.Errorf("failed to delete lock node %s: %v", fullPath, err)
	}
}

func (z *zkDCS) create(path string, val interface{}, flags int32) error {
	fullPath := z.buildFullPath(path)
	data, err := json.Marshal(val)
	if err != nil {
		panic(fmt.Sprintf("failed to serialize to JSON %#v", val))
	}
	_, err = z.retryCreate(fullPath, data, flags, nil)
	if err != nil {
		if err == zk.ErrNodeExists {
			return ErrExists
		}
		z.logger.Errorf("failed to create node %s with %+v: %v", fullPath, val, err)
	}
	return err
}

func (z *zkDCS) Create(path string, val interface{}) error {
	return z.create(path, val, 0)
}

func (z *zkDCS) CreateEphemeral(path string, val interface{}) error {
	return z.create(path, val, zk.FlagEphemeral)
}

func (z *zkDCS) set(path string, val interface{}, flags int32) error {
	fullPath := z.buildFullPath(path)
	data, err := json.Marshal(val)
	if err != nil {
		panic(fmt.Sprintf("failed to serialize to JSON %#v", val))
	}
	_, stat, err := z.retryGet(fullPath)
	if err != nil && err != zk.ErrNoNode {
		z.logger.Errorf("failed to get node %s: %v", fullPath, err)
		return err
	}
	if err == zk.ErrNoNode {
		parts := strings.Split(fullPath, sep)
		err = z.makePath(strings.Join(parts[:len(parts)-1], sep))
		if err != nil {
			return err
		}
		_, err = z.retryCreate(fullPath, data, flags, nil)
		if err != nil {
			z.logger.Errorf("failed to create node %s with %v: %v", fullPath, val, err)
		}
		return err
	}
	if flags&zk.FlagEphemeral != 0 && stat.EphemeralOwner == 0 {
		return fmt.Errorf("node %s exists, but not ephemeral, can't make it ephemeral", path)
	}
	_, err = z.retrySet(fullPath, data, stat.Version)
	if err != nil {
		z.logger.Errorf("failed to set node %s to %+v: %v", fullPath, val, err)
	}
	return err
}

func (z *zkDCS) Set(path string, val interface{}) error {
	return z.set(path, val, 0)
}

func (z *zkDCS) SetEphemeral(path string, val interface{}) error {
	return z.set(path, val, zk.FlagEphemeral)
}

func (z *zkDCS) Delete(path string) error {
	fullPath := z.buildFullPath(path)
	_, stat, err := z.retryGet(fullPath)
	if err == zk.ErrNoNode {
		return nil
	}
	if err != nil {
		z.logger.Errorf("failed to get node %s: %v", fullPath, err)
		return err
	}
	err = z.retryDelete(fullPath, stat.Version)
	if err != nil {
		z.logger.Errorf("failed to delete node %s: %v", fullPath, err)
	}
	return err
}

func (z *zkDCS) Get(path string, dest interface{}) error {
	fullPath := z.buildFullPath(path)
	data, _, err := z.retryGet(fullPath)
	if err == zk.ErrNoNode {
		return ErrNotFound
	}
	if err != nil {
		z.logger.Errorf("failed to get node %s: %v", fullPath, err)
		return err
	}
	if err = json.Unmarshal(data, dest); err != nil {
		z.logger.Errorf("malformed node data %s (%s): %v", fullPath, data, err)
		return ErrMalformed
	}
	return nil
}

func (z *zkDCS) GetTree(path string) (interface{}, error) {
	fullPath := z.buildFullPath(path)
	children, _, err := z.retryChildren(fullPath)
	if err != nil {
		z.logger.Errorf("failed to get children of %s: %v", fullPath, err)
		return nil, err
	}
	if len(children) == 0 {
		var data []byte
		data, _, err = z.retryGet(fullPath)
		if err != nil {
			z.logger.Errorf("failed to get data of %s: %v", fullPath, err)
			return nil, err
		}
		if len(data) == 0 {
			return nil, nil
		}
		var ret interface{}
		err = json.Unmarshal(data, &ret)
		if err != nil {
			z.logger.Errorf("malformed node data %s (%s): %v", fullPath, data, err)
			return nil, err
		}
		return ret, nil
	}
	ret := make(map[string]interface{})
	for _, name := range children {
		ret[name], err = z.GetTree(JoinPath(path, name))
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func (z *zkDCS) GetChildren(path string) ([]string, error) {
	fullPath := z.buildFullPath(path)
	children, _, err := z.retryChildren(fullPath)
	if err == zk.ErrNoNode {
		return nil, ErrNotFound
	}
	if err != nil {
		z.logger.Errorf("failed to get children of %s: %v", fullPath, err)
		return nil, err
	}
	return children, nil
}

func (z *zkDCS) Close() {
	z.conn.Close()
}
