package dcs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildFullPath(t *testing.T) {
	z := &zkDCS{config: &ZookeeperConfig{Namespace: "//abc//def"}}
	require.Equal(t, "/abc/def/xyz", z.buildFullPath("/xyz/"))
	require.Equal(t, "/abc/def/xyz", z.buildFullPath("xyz"))
	require.Equal(t, "/abc/def/xyz", z.buildFullPath("////xyz////"))
	require.Equal(t, "/abc/def", z.buildFullPath(""))
	z = &zkDCS{config: &ZookeeperConfig{Namespace: "//abc//def/"}}
	require.Equal(t, "/abc/def/xyz", z.buildFullPath("/xyz/"))
	require.Equal(t, "/abc/def/xyz", z.buildFullPath("xyz"))
	require.Equal(t, "/abc/def/xyz", z.buildFullPath("////xyz////"))
	require.Equal(t, "/abc/def", z.buildFullPath(""))
	z = &zkDCS{config: &ZookeeperConfig{Namespace: "////"}}
	require.Equal(t, "/", z.buildFullPath(""))
	require.Equal(t, "/", z.buildFullPath("///"))
	z = &zkDCS{config: &ZookeeperConfig{Namespace: ""}}
	require.Equal(t, "/", z.buildFullPath(""))
}
