package mysql

import (
	"fmt"
	"sort"
	"sync"

	"github.com/yandex/mysync/internal/config"
	"github.com/yandex/mysync/internal/dcs"
	"github.com/yandex/mysync/internal/log"
)

// comment justifying the following import

// Cluster is a simple collection, containing set of MySQL ha_nodes
type Cluster struct {
	sync.Mutex
	config       *config.Config
	logger       *log.Logger
	local        *Node
	dcs          dcs.DCS
	haNodes      map[string]*Node
	cascadeNodes map[string]*Node
}

func (c *Cluster) IsHAHost(hostname string) bool {
	c.Lock()
	defer c.Unlock()

	_, isOk := c.haNodes[hostname]
	return isOk
}

func (c *Cluster) IsCascadeHost(hostname string) bool {
	c.Lock()
	defer c.Unlock()

	_, isOk := c.cascadeNodes[hostname]
	return isOk
}

func (c *Cluster) GetClusterHAFqdnsFromDcs() ([]string, error) {
	fqdns, err := c.dcs.GetChildren(dcs.PathHANodesPrefix)
	if err == dcs.ErrNotFound {
		return make([]string, 0), nil
	}
	if err != nil {
		return nil, err
	}

	return fqdns, nil
}

func (c *Cluster) GetClusterHAHostsFromDcs() (map[string]NodeConfiguration, error) {
	result := make(map[string]NodeConfiguration)
	hosts, err := c.GetClusterHAFqdnsFromDcs()
	if err != nil {
		return nil, err
	}
	for _, host := range hosts {
		nc, err := c.GetNodeConfiguration(host)
		if err != nil {
			return nil, err
		}
		result[host] = *nc
	}
	return result, nil
}

func (c *Cluster) GetClusterCascadeFqdnsFromDcs() ([]string, error) {
	fqdns, err := c.dcs.GetChildren(dcs.PathCascadeNodesPrefix)
	if err == dcs.ErrNotFound {
		return make([]string, 0), nil
	}
	if err != nil {
		return nil, err
	}

	return fqdns, nil
}

func (c *Cluster) GetClusterCascadeHostsFromDcs() (map[string]string, error) {
	result := make(map[string]string)
	hosts, err := c.GetClusterCascadeFqdnsFromDcs()
	if err != nil {
		return nil, err
	}
	for _, host := range hosts {
		var cns CascadeNodeConfiguration
		err = c.dcs.Get(dcs.JoinPath(dcs.PathCascadeNodesPrefix, host), &cns)
		if err != nil {
			return nil, err
		}
		result[host] = cns.StreamFrom
	}
	return result, nil
}

func (c *Cluster) registerLocalNode() error {
	if c.local == nil {
		node, err := NewNode(c.config, c.logger, c.config.Hostname)
		if err != nil {
			c.Close()
			return fmt.Errorf("failed to configure local node due (%v)", err)
		}
		// just configure local node, but do not register it in cluster
		c.local = node
	}
	return nil
}

// NewCluster connects (lazy) to MySQL ha_nodes and returns new Cluster
func NewCluster(config *config.Config, logger *log.Logger, dcs dcs.DCS) (*Cluster, error) {
	c := &Cluster{
		config:       config,
		logger:       logger,
		haNodes:      make(map[string]*Node),
		cascadeNodes: make(map[string]*Node),
		local:        nil,
		dcs:          dcs,
	}
	err := RegisterTLSConfig(config)
	if err != nil {
		return nil, err
	}
	// try to configure local node - we dont need dcs to do that
	if err = c.registerLocalNode(); err != nil {
		return nil, err
	}
	return c, err
}

// Close closes all established connections to MySQL ha_nodes
func (c *Cluster) Close() {
	c.Lock()
	defer c.Unlock()

	for _, node := range c.haNodes {
		_ = node.Close()
	}
}

// Get returns MySQL Node by host name
func (c *Cluster) Get(host string) *Node {
	c.Lock()
	defer c.Unlock()

	result, ok := c.haNodes[host]
	if ok {
		return result
	}
	return c.cascadeNodes[host]
}

// Local returns a MySQL Node running on the same host as the current mysync process
func (c *Cluster) Local() *Node {
	return c.local
}

// Read host names from DCS and updates cluster obj
func (c *Cluster) UpdateHostsInfo() error {
	c.Lock()
	defer c.Unlock()

	err := c.updateHAHostsInfo()
	if err != nil {
		return err
	}
	err = c.updateCascadeHostsInfo()
	if err != nil {
		return err
	}
	return nil
}

func (c *Cluster) updateHAHostsInfo() error {
	hosts, err := c.GetClusterHAFqdnsFromDcs()
	if err != nil {
		return err
	}
	c.logger.Infof("HA nodes: %s", hosts)
	set := make(map[string]int, len(hosts))
	for _, host := range hosts {
		set[host]++
	}

	for host := range set {
		if _, found := c.haNodes[host]; !found {
			var node *Node
			if c.local.host == host {
				node = c.local
			} else if node, err = NewNode(c.config, c.logger, host); err != nil {
				return err
			}
			c.haNodes[node.Host()] = node
		}
	}
	// we delete hosts which are no longer in dcs
	for hostname := range c.haNodes {
		if _, found := set[hostname]; !found {
			if hostname != c.local.host {
				if err = c.haNodes[hostname].Close(); err != nil {
					return err
				}
			}
			delete(c.haNodes, hostname)
		}
	}
	return nil
}

func (c *Cluster) updateCascadeHostsInfo() error {
	hosts, err := c.GetClusterCascadeHostsFromDcs()
	if err != nil {
		return err
	}

	hostList := make([]string, 0)
	for host := range hosts {
		hostList = append(hostList, host)
	}
	c.logger.Infof("cascade nodes: %s", hostList)

	for host := range hosts {
		if _, found := c.cascadeNodes[host]; !found {
			var node *Node
			if c.local.host == host {
				node = c.local
			} else if node, err = NewNode(c.config, c.logger, host); err != nil {
				return err
			}
			c.cascadeNodes[node.Host()] = node
		}
	}
	// we delete hosts which are no longer in dcs
	for hostname := range c.cascadeNodes {
		if _, found := hosts[hostname]; !found {
			if hostname != c.local.host {
				if err = c.cascadeNodes[hostname].Close(); err != nil {
					return err
				}
			}
			delete(c.cascadeNodes, hostname)
		}
	}
	return nil
}

// HANodeHosts returns list of mysql HA-host names
func (c *Cluster) HANodeHosts() []string {
	c.Lock()
	defer c.Unlock()

	var hosts []string
	for host := range c.haNodes {
		hosts = append(hosts, host)
	}
	sort.Strings(hosts)
	return hosts
}

func (c *Cluster) CascadeNodeHosts() []string {
	c.Lock()
	defer c.Unlock()

	var hosts []string
	for host := range c.cascadeNodes {
		hosts = append(hosts, host)
	}
	sort.Strings(hosts)
	return hosts
}

func (c *Cluster) AllNodeHosts() []string {
	c.Lock()
	defer c.Unlock()

	var hosts []string
	for host := range c.cascadeNodes {
		hosts = append(hosts, host)
	}
	for host := range c.haNodes {
		hosts = append(hosts, host)
	}
	sort.Strings(hosts)

	return hosts
}

// PingNode checks connection to host without adding it to cluster
func (c *Cluster) PingNode(host string) (bool, error) {
	node, err := NewNode(c.config, c.logger, host)
	if err != nil {
		return false, err
	}
	ok, err := node.Ping()
	if err != nil && IsErrorDubious(err) {
		return false, err
	}
	return ok, nil
}

func (c *Cluster) GetNodeConfiguration(host string) (*NodeConfiguration, error) {
	var nc NodeConfiguration
	err := c.dcs.Get(dcs.JoinPath(dcs.PathHANodesPrefix, host), &nc)
	if err != nil {
		if err != dcs.ErrNotFound && err != dcs.ErrMalformed {
			return nil, fmt.Errorf("failed to get Priority for host %s: %s", host, err)
		}
		return defaultNodeConfiguration(), nil
	}

	return &nc, nil
}

func defaultNodeConfiguration() *NodeConfiguration {
	return &NodeConfiguration{Priority: 0}
}
