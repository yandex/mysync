package dcs

import (
	"os"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// ZookeeperConfig contains Zookeeper connection info
type ZookeeperConfig struct {
	CACert                string                   `config:"ca_cert" yaml:"ca_cert"`
	Namespace             string                   `config:"namespace,required"`
	Hostname              string                   `config:"hostname" yaml:"hostname"`
	CertFile              string                   `config:"certfile" yaml:"certfile"`
	KeyFile               string                   `config:"keyfile" yaml:"keyfile"`
	Password              string                   `config:"password" yaml:"password"`
	Username              string                   `config:"username" yaml:"username"`
	Hosts                 []string                 `config:"hosts,required"`
	RandomHostProvider    RandomHostProviderConfig `config:"random_host_provider" yaml:"random_host_provider"`
	BackoffInterval       time.Duration            `config:"backoff_interval" yaml:"backoff_interval"`
	BackoffMaxRetries     uint64                   `config:"backoff_max_retries" yaml:"backoff_max_retries"`
	BackoffMaxElapsedTime time.Duration            `config:"backoff_max_elapsed_time" yaml:"backoff_max_elapsed_time"`
	BackoffMaxInterval    time.Duration            `config:"backoff_max_interval" yaml:"backoff_max_interval"`
	BackoffMultiplier     float64                  `config:"backoff_multiplier" yaml:"backoff_multiplier"`
	BackoffRandFactor     float64                  `config:"backoff_rand_factor" yaml:"backoff_rand_factor"`
	SessionTimeout        time.Duration            `config:"session_timeout" yaml:"session_timeout"`
	Auth                  bool                     `config:"auth" yaml:"auth"`
	UseSSL                bool                     `config:"use_ssl" yaml:"use_ssl"`
	VerifyCerts           bool                     `config:"verify_certs" yaml:"verify_certs"`
}

type RandomHostProviderConfig struct {
	LookupTimeout      time.Duration `config:"lookup_timeout" yaml:"lookup_timeout"`
	LookupTTL          time.Duration `config:"lookup_ttl" yaml:"lookup_ttl"`
	LookupTickInterval time.Duration `config:"lookup_tick_interval" yaml:"lookup_tick_interval"`
}

func DefaultRandomHostProviderConfig() RandomHostProviderConfig {
	return RandomHostProviderConfig{
		LookupTimeout:      3 * time.Second,
		LookupTTL:          300 * time.Second,
		LookupTickInterval: 60 * time.Second,
	}
}

// DefaultZookeeperConfig return default Zookeeper connection configuration
func DefaultZookeeperConfig() (ZookeeperConfig, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return ZookeeperConfig{}, err
	}
	config := ZookeeperConfig{
		Hostname:              hostname,
		SessionTimeout:        2 * time.Second,
		BackoffInterval:       backoff.DefaultInitialInterval,
		BackoffRandFactor:     backoff.DefaultRandomizationFactor,
		BackoffMultiplier:     backoff.DefaultMultiplier,
		BackoffMaxInterval:    backoff.DefaultMaxInterval,
		BackoffMaxElapsedTime: backoff.DefaultMaxElapsedTime,
		BackoffMaxRetries:     10,
		RandomHostProvider:    DefaultRandomHostProviderConfig(),
	}
	return config, nil
}
