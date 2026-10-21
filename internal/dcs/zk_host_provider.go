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
	lastLookup time.Time
	resolved   []string
}

type zkaddr struct {
	host string
	addr string
}

type RandomHostProvider struct {
	ctx                      context.Context
	resolver                 *net.Resolver
	tried                    map[string]struct{}
	logger                   *log.Logger
	hosts                    sync.Map
	preferred                zkaddr
	hostsKeys                []string
	retryJitter              time.Duration
	connectivityCheckTimeout time.Duration
	lookupTTL                time.Duration
	lookupTimeout            time.Duration
	lookupTickInterval       time.Duration
	useAddrs                 bool
	isRetry                  bool
}

func NewRandomHostProvider(ctx context.Context, config *RandomHostProviderConfig, useAddrs bool, logger *log.Logger) *RandomHostProvider {
	return &RandomHostProvider{
		ctx:                      ctx,
		lookupTTL:                config.LookupTTL,
		connectivityCheckTimeout: config.ConnectivityCheckTimeout,
		lookupTimeout:            config.LookupTimeout,
		lookupTickInterval:       config.LookupTickInterval,
		retryJitter:              config.RetryJitter,
		logger:                   logger,
		tried:                    make(map[string]struct{}),
		hosts:                    sync.Map{},
		resolver:                 &net.Resolver{},
		useAddrs:                 useAddrs,
	}
}

func (rhp *RandomHostProvider) Init(servers []string) error {
	rhp.hostsKeys = nil
	rhp.hosts = sync.Map{}
	rhp.preferred = zkaddr{}
	var allResolvedServers []zkaddr

	for _, host := range servers {
		resolved, err := rhp.resolveHost(host)
		if err != nil {
			rhp.logger.Error().Err(err).Msgf("host definition %s is invalid", host)
			continue
		}

		for _, addr := range resolved {
			allResolvedServers = append(allResolvedServers, zkaddr{host: host, addr: addr})
		}

		rhp.hosts.Store(host, zkhost{
			resolved:   resolved,
			lastLookup: time.Now(),
		})
		rhp.hostsKeys = append(rhp.hostsKeys, host)
	}

	if len(allResolvedServers) == 0 {
		return fmt.Errorf("unable to resolve any host from %v", servers)
	}

	preferred, err := rhp.checkZKConnectivity(allResolvedServers)
	if err != nil {
		return err
	}
	rhp.preferred = preferred

	go rhp.resolveHosts()

	return nil
}

func (rhp *RandomHostProvider) checkZKConnectivity(servers []zkaddr) (zkaddr, error) {
	if len(servers) == 0 {
		return zkaddr{}, fmt.Errorf("no servers available for connectivity check")
	}

	ctx, cancel := context.WithTimeout(rhp.ctx, rhp.connectivityCheckTimeout)
	defer cancel()

	type checkResult struct {
		err    error
		server zkaddr
	}
	results := make(chan checkResult, len(servers))
	for _, server := range servers {
		go func() {
			var dialer net.Dialer
			conn, err := dialer.DialContext(ctx, "tcp", server.addr)
			if err == nil {
				conn.Close()
			}
			results <- checkResult{server: server, err: err}
		}()
	}

	for range servers {
		res := <-results
		if res.err == nil {
			rhp.logger.Info().Str("server", res.server.addr).Msg("zk connectivity check succeeded")
			return res.server, nil
		}
		rhp.logger.Error().Str("server", res.server.addr).Err(res.err).Msg("connectivity check failed")
	}

	return zkaddr{}, fmt.Errorf("failed to connect to any zk server: all attempts timed out or refused")
}

func (rhp *RandomHostProvider) resolveHosts() {
	ticker := time.NewTicker(rhp.lookupTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, pair := range rhp.hostsKeys {
				host, _ := rhp.hosts.Load(pair)
				zhost := host.(zkhost)

				if len(zhost.resolved) == 0 || time.Since(zhost.lastLookup) > rhp.lookupTTL {
					resolved, err := rhp.resolveHost(pair)
					if err != nil || len(resolved) == 0 {
						rhp.logger.Error().Err(err).Msgf("background resolve for %s failed", pair)
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
		rhp.logger.Error().Err(err).Msgf("unable to resolve %s", host)
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
	if rhp.ctx.Err() != nil {
		rhp.isRetry = false
		return rhp.hostsKeys[0], true
	}
	if rhp.isRetry {
		v := time.Duration(rand.Float64() * float64(rhp.retryJitter))
		rhp.logger.Info().Msgf("Triggering connection retry jitter: %v", v)
		timer := time.NewTimer(v)
		select {
		case <-timer.C:
		case <-rhp.ctx.Done():
			timer.Stop()
			rhp.isRetry = false
			return rhp.hostsKeys[0], true
		}
		rhp.isRetry = false
	}
	if rhp.ctx.Err() != nil {
		rhp.isRetry = false
		return rhp.hostsKeys[0], true
	}

	if rhp.preferred.host != "" {
		preferred := rhp.preferred
		rhp.preferred = zkaddr{}
		rhp.tried[preferred.host] = struct{}{}
		if rhp.useAddrs {
			return preferred.addr, false
		}
		return preferred.host, false
	}

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

	if needRetry {
		rhp.isRetry = true
	}

	return ret, needRetry
}

func (rhp *RandomHostProvider) Connected() {
	for k := range rhp.tried {
		delete(rhp.tried, k)
	}
}
