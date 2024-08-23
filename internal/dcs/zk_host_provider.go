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
	hostsKeys          []string
	tried              map[string]struct{}
	logger             *log.Logger
	lookupTTL          time.Duration
	lookupTimeout      time.Duration
	lookupTickInterval time.Duration
	resolver           *net.Resolver
}

func NewRandomHostProvider(ctx context.Context, config *RandomHostProviderConfig, logger *log.Logger) *RandomHostProvider {
	return &RandomHostProvider{
		ctx:                ctx,
		lookupTTL:          config.LookupTTL,
		lookupTimeout:      config.LookupTimeout,
		lookupTickInterval: config.LookupTickInterval,
		logger:             logger,
		tried:              make(map[string]struct{}),
		hosts:              sync.Map{},
		resolver:           &net.Resolver{},
	}
}

func (rhp *RandomHostProvider) Init(servers []string) error {
	numResolved := 0

	for _, host := range servers {
		resolved, err := rhp.resolveHost(host)
		if err != nil {
			rhp.logger.Errorf("host definition %s is invalid %v", host, err)
			continue
		}
		numResolved += len(resolved)
		rhp.hosts.Store(host, zkhost{
			resolved:   resolved,
			lastLookup: time.Now(),
		})
		rhp.hostsKeys = append(rhp.hostsKeys, host)
	}

	if numResolved == 0 {
		return fmt.Errorf("unable to resolve any host from %v", servers)
	}

	go rhp.resolveHosts()

	return nil
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
			ret = zhost.resolved[rand.Intn(len(zhost.resolved))]
		}
	}

	return ret, needRetry
}

func (rhp *RandomHostProvider) Connected() {
	for k := range rhp.tried {
		delete(rhp.tried, k)
	}
}
