package dcs

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"time"

	"github.com/go-zookeeper/zk"
)

func CreateTLSConfig(rootCAFile, certFile, keyFile string) (*tls.Config, error) {
	rootCABytes, err := os.ReadFile(rootCAFile)
	if err != nil {
		return nil, err
	}

	rootCA := x509.NewCertPool()
	ok := rootCA.AppendCertsFromPEM(rootCABytes)
	if !ok {
		return nil, err
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      rootCA,
	}, nil
}

func GetTLSDialer(dialer *net.Dialer, tlsConfig *tls.Config) (zk.Dialer, error) {
	return func(network, address string, _ time.Duration) (net.Conn, error) {
		return tls.DialWithDialer(dialer, network, address, tlsConfig)
	}, nil
}
