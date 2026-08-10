package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersionCheckIfVersionReplicaStatus(t *testing.T) {
	testCases := []struct {
		name     string
		version  Version
		expected bool
	}{
		{name: "5.7", version: Version{MajorVersion: 5, MinorVersion: 7, PatchVersion: 44}, expected: false},
		{name: "8.0.21", version: Version{MajorVersion: 8, MinorVersion: 0, PatchVersion: 21}, expected: false},
		{name: "8.0.22", version: Version{MajorVersion: 8, MinorVersion: 0, PatchVersion: 22}, expected: true},
		{name: "8.4", version: Version{MajorVersion: 8, MinorVersion: 4, PatchVersion: 0}, expected: true},
		{name: "9.7", version: Version{MajorVersion: 9, MinorVersion: 7, PatchVersion: 1}, expected: true},
		{name: "future major", version: Version{MajorVersion: 10, MinorVersion: 0, PatchVersion: 0}, expected: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, testCase.version.CheckIfVersionReplicaStatus())
		})
	}
}

func TestVersionCheckIfExternalReplicationSupported(t *testing.T) {
	testCases := []struct {
		name     string
		version  Version
		expected bool
	}{
		{name: "5.7", version: Version{MajorVersion: 5, MinorVersion: 7, PatchVersion: 44}, expected: false},
		{name: "8.0.21", version: Version{MajorVersion: 8, MinorVersion: 0, PatchVersion: 21}, expected: false},
		{name: "8.0.22", version: Version{MajorVersion: 8, MinorVersion: 0, PatchVersion: 22}, expected: true},
		{name: "8.4", version: Version{MajorVersion: 8, MinorVersion: 4, PatchVersion: 0}, expected: true},
		{name: "9.7", version: Version{MajorVersion: 9, MinorVersion: 7, PatchVersion: 1}, expected: true},
		{name: "future major", version: Version{MajorVersion: 10, MinorVersion: 0, PatchVersion: 0}, expected: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, testCase.version.CheckIfExternalReplicationSupported())
		})
	}
}
