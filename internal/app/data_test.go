package app

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStringerWorksOnNodeState(t *testing.T) {
	ns := &NodeState{}
	nsStr := fmt.Sprintf("%v", ns)
	if nsStr != "<ping=ERR repl=??? sync=??? ro=false offline=false lag=0.00 du=??? cr=??? gtid=??? turbo=false>" {
		t.Errorf("%s", ns)
	}

	ns.IsMaster = false
	ns.MasterState = new(MasterState)
	ns.MasterState.ExecutedGtidSet = "6DBC0B04-4B09-43DC-86CC-9AF852DED919:1-101"
	ns.SlaveState = new(SlaveState)
	ns.SlaveState.ExecutedGtidSet = "6DBC0B04-4B09-43DC-86CC-9AF852DED919:1-40"

	nsStr = fmt.Sprintf("%v", ns)

	require.Equal(
		t,
		"<ping=ERR repl= sync=??? ro=false offline=false lag=NaN du=??? cr=??? gtid=6DBC0B04-4B09-43DC-86CC-9AF852DED919:1-40 turbo=false>",
		nsStr,
	)

	ns.ShowOnlyGTIDDiff = true
	nsStr = fmt.Sprintf("%v", ns)

	require.Equal(
		t,
		"<ping=ERR repl= sync=??? ro=false offline=false lag=NaN du=??? cr=??? gtid=source ahead on: 6dbc0b04-4b09-43dc-86cc-9af852ded919:41-101 turbo=false>",
		nsStr,
	)
}

func TestStringerWorksOnNodeStateMap(t *testing.T) {
	m := make(map[string]*NodeState)
	m["a"] = &NodeState{}
	m["b"] = &NodeState{}
	m["c"] = &NodeState{}

	mStr := fmt.Sprintf("%v", m)

	require.Equal(
		t,
		"map[a:<ping=ERR repl=??? sync=??? ro=false offline=false lag=0.00 du=??? cr=??? gtid=??? turbo=false> b:<ping=ERR repl=??? sync=??? ro=false offline=false lag=0.00 du=??? cr=??? gtid=??? turbo=false> c:<ping=ERR repl=??? sync=??? ro=false offline=false lag=0.00 du=??? cr=??? gtid=??? turbo=false>]",
		mStr,
	)
}

func newMockNodeState() *NodeState {
	return &NodeState{
		SlaveState: &SlaveState{
			MasterLogFile: "test_master_log_file",
			MasterLogPos:  2,
		},
	}
}

func TestUpdateBinlogWithChanges(t *testing.T) {
	oldLogFile := "test_master_log_file"
	maxLogPos := int64(1)

	ns := newMockNodeState()

	oldLogFile, maxLogPos = ns.UpdateBinlogStatus(oldLogFile, maxLogPos)

	require.Equal(t, "test_master_log_file", oldLogFile)
	require.Equal(t, int64(2), maxLogPos)
	require.Equal(t, true, ns.IsLoadingBinlog)
}

func TestUpdateBinlogWithoutChanges(t *testing.T) {
	oldLogFile := "test_master_log_file"
	maxLogPos := int64(2)

	ns := newMockNodeState()

	oldLogFile, maxLogPos = ns.UpdateBinlogStatus(oldLogFile, maxLogPos)

	require.Equal(t, "test_master_log_file", oldLogFile)
	require.Equal(t, int64(2), maxLogPos)
	require.Equal(t, false, ns.IsLoadingBinlog)
}

func TestUpdateBinlogAfterReloading(t *testing.T) {
	oldLogFile := "test_master_log_file"
	maxLogPos := int64(5)

	ns := newMockNodeState()

	oldLogFile, maxLogPos = ns.UpdateBinlogStatus(oldLogFile, maxLogPos)

	require.Equal(t, "test_master_log_file", oldLogFile)
	require.Equal(t, int64(5), maxLogPos)
	require.Equal(t, false, ns.IsLoadingBinlog)
}

func TestUpdateBinlogAfterMasterSwitch(t *testing.T) {
	oldLogFile := "old_log_file"
	maxLogPos := int64(5)

	ns := newMockNodeState()

	oldLogFile, maxLogPos = ns.UpdateBinlogStatus(oldLogFile, maxLogPos)

	require.Equal(t, "test_master_log_file", oldLogFile)
	require.Equal(t, int64(2), maxLogPos)
	require.Equal(t, false, ns.IsLoadingBinlog)
}
