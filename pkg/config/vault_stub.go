package config

import (
	"crypto/tls"
	"net/http"
	"time"

	vault "github.com/hashicorp/vault/api"
)

// NewVaultClient builds a pre-authenticated Vault client without reading env.
func NewVaultClient(addr, token string, tlsCfg *tls.Config) (*vault.Client, error) {
	conf := vault.DefaultConfig()
	conf.Address = addr

	// Overwrite http client to avoid env defaults and set sane timeouts.
	transport := &http.Transport{
		TLSClientConfig:     tlsCfg,
		DisableCompression:  true,
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	conf.HttpClient = &http.Client{
		Transport: transport,
		Timeout:   15 * time.Second,
	}

	cli, err := vault.NewClient(conf)
	if err != nil {
		return nil, err
	}
	cli.SetToken(token) // or login with Kubernetes/AppRole before calling LoadConfigFromVault
	return cli, nil
}
