package dcs

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/client/v3/concurrency"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/yandex/mysync/internal/log"
)

type etcdDCS struct {
	logger             *log.Logger
	config             *EtcdConfig
	conn               *clientv3.Client
	session            *concurrency.Session
	connectedLock      sync.Mutex
	disconnectCallback func() error
}

func NewEtcd(config *EtcdConfig, logger *log.Logger) (DCS, error) {
	if len(config.Hosts) == 0 {
		return nil, fmt.Errorf("etcd not configured, fill etcd/host in config")
	}
	if config.Namespace == "" {
		return nil, fmt.Errorf("etcd not configured, fill etcd/namespace in config")
	}
	if config.SessionTimeout == 0 {
		return nil, fmt.Errorf("namespace session timeout not configured")
	}
	clientConfig := clientv3.Config{
		Endpoints:   config.Hosts,
		DialTimeout: config.SessionTimeout,
	}
	if config.UseSSL {
		if config.CACert == "" || config.KeyFile == "" || config.CertFile == "" {
			return nil, fmt.Errorf("zookeeper ssl not configured, fill ca_cert/key_file/cert_file in config or disable use_ssl flag")
		}
		tlsConfig, err := CreateTLSConfig(config.CACert, config.CertFile, config.KeyFile)
		if err != nil {
			return nil, err
		}
		clientConfig.TLS = tlsConfig
	}
	if config.Auth {
		if config.Username == "" || config.Password == "" {
			return nil, fmt.Errorf("etcd auth not configured, fill username/password in config or disable auth flag")
		}
		clientConfig.Username = config.Username
		clientConfig.Password = config.Password
	}

	conn, err := clientv3.New(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("can not establish connection")
	}

	session, err := concurrency.NewSession(conn)
	if err != nil {
		return nil, fmt.Errorf("can not create session: %w", err)
	}

	return &etcdDCS{
		logger:             logger,
		config:             config,
		conn:               conn,
		session:            session,
		disconnectCallback: func() error { return nil },
	}, nil
}

func (e *etcdDCS) buildFullPath(path string) string {
	return buildFullPath(e.config.Namespace, path)
}

func (e *etcdDCS) IsConnected() bool { // acquire lock (or wait to have it)
	e.connectedLock.Lock()
	defer e.connectedLock.Unlock()
	for _, ep := range e.conn.Endpoints() {
		if _, err := e.conn.Status(context.TODO(), ep); err == nil {
			return true
		}
	}
	return false
}

func (e *etcdDCS) WaitConnected(timeout time.Duration) bool {
	return e.IsConnected()
}

func (e *etcdDCS) Initialize() {}

func (e *etcdDCS) SetDisconnectCallback(callback func() error) {
	e.connectedLock.Lock()
	defer e.connectedLock.Unlock()
	e.disconnectCallback = callback
}

func (e *etcdDCS) Close() {
	e.conn.Close()
}

func (e *etcdDCS) AcquireLock(path string) bool {
	mu := concurrency.NewMutex(e.session, e.buildFullPath(path))
	err := mu.TryLock(context.TODO())
	if err != nil {
		e.logger.Errorf("try lock failed, error: %v", err)
		return false
	}
	return true
}

func (e *etcdDCS) ReleaseLock(path string) {
	mu := concurrency.NewMutex(e.session, e.buildFullPath(path))
	if err := mu.Unlock(context.TODO()); err != nil {
		e.logger.Errorf("unlock failed, error: %v", err)
	}
}

func (e *etcdDCS) create(path string, val interface{}) error {
	key := e.buildFullPath(path)
	strVal, ok := val.(string)
	if !ok {
		ba, err := json.Marshal(val)
		if err != nil {
			return fmt.Errorf("could not cast value %v to string, error: %w", val, err)
		}
		strVal = string(ba)
	}
	_, err := e.conn.Put(context.TODO(), key, strVal)
	if err != nil {
		return fmt.Errorf("failed to put to etcd key %s, value %s, error: %w", path, strVal, err)
	}
	return nil
}

func (e *etcdDCS) Create(path string, val interface{}) error {
	return e.create(path, val)
}

func (e *etcdDCS) CreateEphemeral(path string, val interface{}) error {
	return e.create(path, val)
}

func (e *etcdDCS) Set(path string, val interface{}) error {
	return e.create(path, val)
}

func (e *etcdDCS) SetEphemeral(path string, val interface{}) error {
	return e.create(path, val)
}

func (e *etcdDCS) Get(path string, dest interface{}) error {
	key := e.buildFullPath(path)
	resp, err := e.conn.Get(context.TODO(), key, clientv3.WithLastRev()...)
	if err != nil {
		return fmt.Errorf("could not get value by key %s, error: %w", path, err)
	}
	if len(resp.Kvs) == 0 {
		return fmt.Errorf("no value found")
	}
	val := resp.Kvs[0].Value
	if err := json.Unmarshal(val, dest); err != nil {
		return fmt.Errorf("could not unmarshal for key %s, value %v, error: %w", path, val, err)
	}
	return nil
}

func (e *etcdDCS) GetTree(path string) (interface{}, error) {
	var dst string
	if err := e.Get(path, &dst); err != nil {
		return nil, fmt.Errorf("could not get tree for path %s", path)
	}
	return dst, nil
}

func (e *etcdDCS) GetChildren(path string) ([]string, error) {
	var dst string
	if err := e.Get(path, &dst); err != nil {
		return nil, fmt.Errorf("could not get children for path %s", path)
	}
	return []string{dst}, nil
}

func (e *etcdDCS) Delete(path string) error {
	key := e.buildFullPath(path)
	_, err := e.conn.Delete(context.TODO(), key)
	if err != nil {
		return fmt.Errorf("could not delete element by key %s", path)
	}
	return nil
}
