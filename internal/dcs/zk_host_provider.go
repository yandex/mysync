package dcs

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/yandex/mysync/internal/log"
)

type zkhost struct {
	resolved   []string
	lastLookup time.Time
}

type RandomHostProvider struct {
	ctx                context.Context
	hosts              sync.Map
	useAddrs           bool
	hostsKeys          []string
	tried              map[string]struct{}
	logger             *log.Logger
	lookupTTL          time.Duration
	lookupTimeout      time.Duration
	lookupTickInterval time.Duration
	resolver           *net.Resolver
}

func NewRandomHostProvider(ctx context.Context, config *RandomHostProviderConfig, useAddrs bool, logger *log.Logger) *RandomHostProvider {
	return &RandomHostProvider{
		ctx:                ctx,
		lookupTTL:          config.LookupTTL,
		lookupTimeout:      config.LookupTimeout,
		lookupTickInterval: config.LookupTickInterval,
		logger:             logger,
		tried:              make(map[string]struct{}),
		hosts:              sync.Map{},
		resolver:           &net.Resolver{},
		useAddrs:           useAddrs,
	}
}

func (rhp *RandomHostProvider) Init(servers []string) error {
	var allResolvedServers []string

	for _, host := range servers {
		resolved, err := rhp.resolveHost(host)
		if err != nil {
			rhp.logger.Errorf("host definition %s is invalid %v", host, err)
			continue
		}

		allResolvedServers = append(allResolvedServers, resolved...)

		rhp.hosts.Store(host, zkhost{
			resolved:   resolved,
			lastLookup: time.Now(),
		})
		rhp.hostsKeys = append(rhp.hostsKeys, host)
	}

	if len(allResolvedServers) == 0 {
		return fmt.Errorf("unable to resolve any host from %v", servers)
	}

	if err := rhp.checkZKConnectivity(allResolvedServers); err != nil {
		return err
	}

	go rhp.resolveHosts()

	return nil
}

func (rhp *RandomHostProvider) checkZKConnectivity(servers []string) error {
	if len(servers) == 0 {
		return fmt.Errorf("no servers available for connectivity check")
	}

	for _, server := range servers {
		conn, err := net.DialTimeout("tcp", server, rhp.lookupTimeout)
		if err == nil {
			conn.Close()
			rhp.logger.Infof("zk connectivity check succeeded for %s", server)
			return nil
		}
		rhp.logger.Errorf("connectivity check failed for %s: %s", server, err)
	}

	return fmt.Errorf("failed to connect to any zk server: all attempts timed out or refused")
}

func (rhp *RandomHostProvider) resolveHosts() {
	ticker := time.NewTicker(rhp.lookupTickInterval)
	for {
		select {
		case <-ticker.C:
			for _, pair := range rhp.hostsKeys {
				host, _ := rhp.hosts.Load(pair)
				zhost := host.(zkhost)

				if len(zhost.resolved) == 0 || time.Since(zhost.lastLookup) > rhp.lookupTTL {
					resolved, err := rhp.resolveHost(pair)
					if err != nil || len(resolved) == 0 {
						rhp.logger.Errorf("background resolve for %s failed: %v", pair, err)
						continue
					}
					rhp.hosts.Store(pair, zkhost{
						resolved:   resolved,
						lastLookup: time.Now(),
					})
				}
			}
		case <-rhp.ctx.Done():
			return
		}
	}
}

func (rhp *RandomHostProvider) resolveHost(pair string) ([]string, error) {
	var res []string
	host, port, err := net.SplitHostPort(pair)
	if err != nil {
		return res, err
	}
	ctx, cancel := context.WithTimeout(rhp.ctx, rhp.lookupTimeout)
	defer cancel()
	addrs, err := rhp.resolver.LookupHost(ctx, host)
	if err != nil {
		rhp.logger.Errorf("unable to resolve %s: %v", host, err)
	}
	for _, addr := range addrs {
		res = append(res, net.JoinHostPort(addr, port))
	}

	return res, nil
}

func (rhp *RandomHostProvider) Len() int {
	return len(rhp.hostsKeys)
}

func (rhp *RandomHostProvider) Next() (server string, retryStart bool) {
	needRetry := false

	var ret string

	for len(ret) == 0 {
		notTried := []string{}

		for _, host := range rhp.hostsKeys {
			if _, ok := rhp.tried[host]; !ok {
				notTried = append(notTried, host)
			}
		}

		var selected string
		if len(notTried) == 0 {
			needRetry = true
			for k := range rhp.tried {
				delete(rhp.tried, k)
			}
			selected = rhp.hostsKeys[rand.Intn(len(rhp.hostsKeys))]
		} else {
			selected = notTried[rand.Intn(len(notTried))]
		}
		rhp.tried[selected] = struct{}{}

		host, _ := rhp.hosts.Load(selected)
		zhost := host.(zkhost)

		if len(zhost.resolved) > 0 {
			if rhp.useAddrs {
				ret = zhost.resolved[rand.Intn(len(zhost.resolved))]
			} else {
				ret = selected
			}
		}
	}

	return ret, needRetry
}

func (rhp *RandomHostProvider) Connected() {
	for k := range rhp.tried {
		delete(rhp.tried, k)
	}
}
