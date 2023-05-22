package dcs

import (
	"os"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// ZookeeperConfig contains Zookeeper connection info
type ZookeeperConfig struct {
	Hostname              string        `config:"hostname" yaml:"hostname"`
	SessionTimeout        time.Duration `config:"session_timeout" yaml:"session_timeout"`
	Namespace             string        `config:"namespace,required"`
	Hosts                 []string      `config:"hosts,required"`
	BackoffInterval       time.Duration `config:"backoff_interval" yaml:"backoff_interval"`
	BackoffRandFactor     float64       `config:"backoff_rand_factor" yaml:"backoff_rand_factor"`
	BackoffMultiplier     float64       `config:"backoff_multiplier" yaml:"backoff_multiplier"`
	BackoffMaxInterval    time.Duration `config:"backoff_max_interval" yaml:"backoff_max_interval"`
	BackoffMaxElapsedTime time.Duration `config:"backoff_max_elapsed_time" yaml:"backoff_max_elapsed_time"`
	BackoffMaxRetries     uint64        `config:"backoff_max_retries" yaml:"backoff_max_retries"`
	Auth                  bool          `config:"auth" yaml:"auth"`
	Username              string        `config:"username" yaml:"username"`
	Password              string        `config:"password" yaml:"password"`
	UseSSL                bool          `config:"use_ssl" yaml:"use_ssl"`
	KeyFile               string        `config:"keyfile" yaml:"keyfile"`
	CertFile              string        `config:"certfile" yaml:"certfile"`
	CACert                string        `config:"ca_cert" yaml:"ca_cert"`
	VerifyCerts           bool          `config:"verify_certs" yaml:"verify_certs"`
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
	}
	return config, nil
}

// EtcdConfig contains etcd connection info
type EtcdConfig struct {
	Hostname       string        `config:"hostname" yaml:"hostname"`
	SessionTimeout time.Duration `config:"session_timeout" yaml:"session_timeout"`
	Namespace      string        `config:"namespace,required"`
	Hosts          []string      `config:"hosts,required"`
	Auth           bool          `config:"auth" yaml:"auth"`
	Username       string        `config:"username" yaml:"username"`
	Password       string        `config:"password" yaml:"password"`
	UseSSL         bool          `config:"use_ssl" yaml:"use_ssl"`
	KeyFile        string        `config:"keyfile" yaml:"keyfile"`
	CertFile       string        `config:"certfile" yaml:"certfile"`
	CACert         string        `config:"ca_cert" yaml:"ca_cert"`
}

// DefaultEtcdConfig return default etcd connection configuration
func DefaultEtcdConfig() (EtcdConfig, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return EtcdConfig{}, err
	}
	config := EtcdConfig{
		Hostname:       hostname,
		SessionTimeout: 2 * time.Second,
	}
	return config, nil
}
