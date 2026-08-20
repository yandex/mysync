package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitEventDefiner(t *testing.T) {
	tests := []struct {
		name         string
		definer      string
		expectedUser string
		expectedHost string
	}{
		{
			name:         "regular definer",
			definer:      "user@host",
			expectedUser: "user",
			expectedHost: "host",
		},
		{
			name:         "at sign in user",
			definer:      "user@domain@host",
			expectedUser: "user@domain",
			expectedHost: "host",
		},
		{
			name:         "definer without host separator",
			definer:      "user",
			expectedUser: "user",
			expectedHost: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			user, host := splitEventDefiner(tt.definer)
			require.Equal(t, tt.expectedUser, user)
			require.Equal(t, tt.expectedHost, host)
		})
	}
}
