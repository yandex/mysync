package dcs

import (
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/yandex/mysync/internal/log"
)

type RandomHostProvider struct {
	lock       sync.Mutex
	servers    []string
	resolved   []string
	tried      map[string]struct{}
	logger     *log.Logger
	lastLookup time.Time
	lookupTTL  time.Duration
}

func NewRandomHostProvider(config *RandomHostProviderConfig, logger *log.Logger) *RandomHostProvider {
	return &RandomHostProvider{
		lookupTTL: config.LookupTTL,
		logger:    logger,
		tried:     make(map[string]struct{}),
	}
}

func (rhp *RandomHostProvider) Init(servers []string) error {
	rhp.lock.Lock()
	defer rhp.lock.Unlock()

	rhp.servers = servers

	err := rhp.resolveHosts()

	if err != nil {
		return fmt.Errorf("failed to init zk host provider %v", err)
	}

	return nil
}

func (rhp *RandomHostProvider) resolveHosts() error {
	resolved := []string{}
	for _, server := range rhp.servers {
		host, port, err := net.SplitHostPort(server)
		if err != nil {
			return err
		}
		addrs, err := net.LookupHost(host)
		if err != nil {
			rhp.logger.Errorf("unable to resolve %s: %v", host, err)
		}
		for _, addr := range addrs {
			resolved = append(resolved, net.JoinHostPort(addr, port))
		}
	}

	if len(resolved) == 0 {
		return fmt.Errorf("no hosts resolved for %q", rhp.servers)
	}

	rhp.lastLookup = time.Now()
	rhp.resolved = resolved

	rand.Shuffle(len(rhp.resolved), func(i, j int) { rhp.resolved[i], rhp.resolved[j] = rhp.resolved[j], rhp.resolved[i] })

	return nil
}

func (rhp *RandomHostProvider) Len() int {
	rhp.lock.Lock()
	defer rhp.lock.Unlock()
	return len(rhp.resolved)
}

func (rhp *RandomHostProvider) Next() (server string, retryStart bool) {
	rhp.lock.Lock()
	defer rhp.lock.Unlock()
	lastTime := time.Since(rhp.lastLookup)
	needRetry := false
	if lastTime > rhp.lookupTTL {
		err := rhp.resolveHosts()
		if err != nil {
			rhp.logger.Errorf("resolve zk hosts failed: %v", err)
		}
	}

	notTried := []string{}

	for _, addr := range rhp.resolved {
		if _, ok := rhp.tried[addr]; !ok {
			notTried = append(notTried, addr)
		}
	}

	var selected string

	if len(notTried) == 0 {
		needRetry = true
		for k := range rhp.tried {
			delete(rhp.tried, k)
		}
		selected = rhp.resolved[rand.Intn(len(rhp.resolved))]
	} else {
		selected = notTried[rand.Intn(len(notTried))]
	}

	rhp.tried[selected] = struct{}{}

	return selected, needRetry
}

func (rhp *RandomHostProvider) Connected() {
	rhp.lock.Lock()
	defer rhp.lock.Unlock()
	for k := range rhp.tried {
		delete(rhp.tried, k)
	}
}
