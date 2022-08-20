package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type TLSConfig struct {
	CertFile      string
	KeyFile       string
	CAFile        string
	ServerAddress string
	Server        bool
}

func SetupTLSConfig(config TLSConfig) (*tls.Config, error) {
	var err error

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13}

	if config.CertFile != "" && config.KeyFile != "" {
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			return nil, err
		}
	}

	if config.CAFile != "" {
		b, err := os.ReadFile(config.CAFile)
		if err != nil {
			return nil, err
		}

		ca := x509.NewCertPool()
		if ok := ca.AppendCertsFromPEM([]byte(b)); !ok {
			return nil, fmt.Errorf("failed to parse root certificate: %q", config.CAFile)
		}

		if config.Server {
			tlsConfig.ClientCAs = ca
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsConfig.RootCAs = ca
		}
		tlsConfig.ServerName = config.ServerAddress
	}

	return tlsConfig, nil
}
