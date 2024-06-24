package app

import (
	"fmt"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/google/uuid"
	"github.com/yandex/mysync/internal/log"
	"github.com/yandex/mysync/internal/mysql"
	"github.com/yandex/mysync/internal/mysql/gtids"
	"github.com/yandex/mysync/internal/util"
)

type nodePosition struct {
	host     string
	gtidset  gtids.GTIDSet
	lag      float64
	priority int64
}

// find most desirable node based on priority and lag
func getMostDesirableNode(logger *log.Logger, positions []nodePosition, priorityChoiceMaxLag time.Duration) (string, error) {
	printHostPriorities(logger, positions)
	maxLagInSeconds := priorityChoiceMaxLag.Seconds()
	mostPriorityNode := getMostPriorityNode(positions)

	if mostPriorityNode == nil {
		return "", fmt.Errorf("destination node not found")
	}
	if mostPriorityNode.lag <= maxLagInSeconds {
		logger.Infof("switchover: host %s selected as highest priority (by lag). Priority: %d, lag: %f (maxLag %f)", mostPriorityNode.host, mostPriorityNode.priority, mostPriorityNode.lag, maxLagInSeconds)
		return mostPriorityNode.host, nil
	}

	// hosts with lag < mostPriorityNode.lag - maxLagInSeconds
	var moreRecentHosts []nodePosition
	thresholdLag := mostPriorityNode.lag - maxLagInSeconds
	for i := 0; i < len(positions); i++ {
		if positions[i].lag < thresholdLag {
			moreRecentHosts = append(moreRecentHosts, positions[i])
		}
	}

	if len(moreRecentHosts) == 0 {
		logger.Infof("switchover: host %s selected as highest priority (no more recent hosts). Priority: %d, lag: %f (maxLag %f)", mostPriorityNode.host, mostPriorityNode.priority, mostPriorityNode.lag, maxLagInSeconds)
		return mostPriorityNode.host, nil
	}

	logger.Infof("switchover: new recurse step, %d hosts with less lag. Skipping host %s, priority: %d, lag: %f (maxLag %f)", len(moreRecentHosts), mostPriorityNode.host, mostPriorityNode.priority, mostPriorityNode.lag, maxLagInSeconds)

	return getMostDesirableNode(logger, moreRecentHosts, priorityChoiceMaxLag)
}

func printHostPriorities(logger *log.Logger, positions []nodePosition) {
	logger.Info("switchover: We will select most desirable node among these:")
	for _, kv := range positions {
		logger.Infof("switchover: host: %s, priority: %d, lag: %f", kv.host, kv.priority, kv.lag)
	}
}

func getMostPriorityNode(positions []nodePosition) *nodePosition {
	switch len(positions) {
	case 0:
		return nil
	case 1:
		return &positions[0]
	default:
		maxPos := positions[0]
		for i := 1; i < len(positions); i++ {
			if maxPos.priority < positions[i].priority {
				maxPos = positions[i]
			} else if maxPos.priority == positions[i].priority {
				if positions[i].gtidset.Equal(maxPos.gtidset) {
					if positions[i].lag < maxPos.lag {
						maxPos = positions[i]
					}
				} else {
					if positions[i].gtidset.Contain(maxPos.gtidset) {
						maxPos = positions[i]
					}
				}
			}
		}
		return &maxPos
	}
}

// Get most recent node based on replication lag and GTID sets
func findMostRecentNodeAndDetectSplitbrain(positions []nodePosition) (string, gtids.GTIDSet, bool) {
	maxPos := positions[0]
	for i := 1; i < len(positions); i++ {
		if positions[i].gtidset.Equal(maxPos.gtidset) {
			if positions[i].lag < maxPos.lag {
				maxPos = positions[i]
			}
		} else {
			if positions[i].gtidset.Contain(maxPos.gtidset) {
				maxPos = positions[i]
			}
		}
	}

	splitBrain := detectSplitbrain(positions, maxPos)
	if splitBrain {
		return "", nil, true
	}

	return maxPos.host, maxPos.gtidset, false
}

func detectSplitbrain(positions []nodePosition, selectedPos nodePosition) bool {
	for i := 0; i < len(positions); i++ {
		if !selectedPos.gtidset.Contain(positions[i].gtidset) {
			return true
		}
	}

	return false
}

func filterOut(a, b []string) (res []string) {
	for _, i := range a {
		if !util.ContainsString(b, i) {
			res = append(res, i)
		}
	}
	return
}

func filterOutNodeFromPositions(positions []nodePosition, hostToFilterOut string) []nodePosition {
	res := []nodePosition{}
	for _, pos := range positions {
		if pos.host != hostToFilterOut {
			res = append(res, pos)
		}
	}
	return res
}

func countAliveHASlavesWithinNodes(nodes []string, clusterState map[string]*NodeState) int {
	cnt := 0
	for _, hostname := range nodes {
		state, ok := clusterState[hostname]
		if ok && state.PingOk && state.SlaveState != nil && !state.IsCascade {
			cnt++
		}
	}
	return cnt
}

func countRunningHASlaves(clusterState map[string]*NodeState) int {
	cnt := 0
	for _, state := range clusterState {
		if state.PingOk && state.SlaveState != nil && !state.IsCascade &&
			state.SlaveState.ReplicationState == mysql.ReplicationRunning {
			cnt++
		}
	}
	return cnt
}

func countHANodes(clusterState map[string]*NodeState) int {
	cnt := 0
	for _, state := range clusterState {
		if !state.IsCascade {
			cnt++
		}
	}
	return cnt
}

func calcLagBytes(binlogs []mysql.Binlog, masterFile string, masterPos int64) int64 {
	var lag int64
	for _, binlog := range binlogs {
		if binlog.Name > masterFile {
			lag += binlog.Size
		} else if masterFile == binlog.Name {
			if binlog.Size > masterPos {
				lag += binlog.Size - masterPos
			}
		}
	}
	return lag
}

func getDubiousHAHosts(clusterState map[string]*NodeState) []string {
	var dubious []string
	for host, state := range clusterState {
		if !state.PingOk && state.PingDubious && !state.IsCascade {
			dubious = append(dubious, host)
		}
	}
	return dubious
}

func getNodeStatesInParallel(hosts []string, getter func(string) (*NodeState, error)) (map[string]*NodeState, error) {
	type result struct {
		name  string
		state *NodeState
		err   error
	}
	results := make(chan result, len(hosts))
	for _, host := range hosts {
		go func(host string) {
			state, err := getter(host)
			results <- result{host, state, err}
		}(host)
	}
	clusterState := make(map[string]*NodeState)
	var err error
	for range hosts {
		result := <-results
		if result.err != nil {
			err = result.err
		} else {
			clusterState[result.name] = result.state
		}
	}
	if err != nil {
		return nil, err
	}

	// adding information about source to each replica
	for host := range clusterState {
		if clusterState[host].SlaveState == nil {
			continue
		}
		masterHost := clusterState[host].SlaveState.MasterHost
		clusterState[host].MasterState = clusterState[masterHost].MasterState
	}
	return clusterState, nil
}

func isSlavePermanentlyLost(sstatus mysql.ReplicaStatus, masterGtidSet gtids.GTIDSet) bool {
	if sstatus.ReplicationState() == mysql.ReplicationError {
		return true
	}
	slaveGtidSet := gtids.ParseGtidSet(sstatus.GetExecutedGtidSet())
	return !isGTIDLessOrEqual(slaveGtidSet, masterGtidSet)
}

func isGTIDLessOrEqual(slaveGtidSet, masterGtidSet gtids.GTIDSet) bool {
	return masterGtidSet.Contain(slaveGtidSet) || masterGtidSet.Equal(slaveGtidSet)
}

func isSplitBrained(slaveGtidSet, masterGtidSet gtids.GTIDSet, masterUUID uuid.UUID) bool {
	mysqlSlaveGtidSet := slaveGtidSet.(*gomysql.MysqlGTIDSet)
	mysqlMasterGtidSet := masterGtidSet.(*gomysql.MysqlGTIDSet)
	for _, slaveSet := range mysqlSlaveGtidSet.Sets {
		masterSet, ok := mysqlMasterGtidSet.Sets[slaveSet.SID.String()]
		if !ok {
			return true
		}

		if masterSet.Contain(slaveSet) {
			continue
		}

		if masterSet.SID == masterUUID {
			continue
		}

		return true
	}

	return false
}

func validatePriority(priority *int64) error {
	if priority == nil || *priority >= 0 {
		return nil
	}

	return fmt.Errorf("priority must be >= 0")
}
