package testutil_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/yandex/mysync/tests/testutil"
)

func TestEventuallyChecksAtDeadline(t *testing.T) {
	calls := 0
	matched := testutil.Eventually(func() bool {
		calls++
		return calls == 2
	}, 0, time.Hour)

	require.True(t, matched)
	require.Equal(t, 2, calls)
}

func TestConsistentlyChecksAtDeadline(t *testing.T) {
	calls := 0
	matched := testutil.Consistently(func() bool {
		calls++
		return calls == 1
	}, 0, time.Hour)

	require.False(t, matched)
	require.Equal(t, 2, calls)
}
