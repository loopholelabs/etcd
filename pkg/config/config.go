/*
	Copyright 2023 Loophole Labs

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		   http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/

package config

import (
	"errors"
	"fmt"
	"github.com/loopholelabs/etcd"
	"github.com/loopholelabs/etcd/pkg/tlsconfig"
	"github.com/spf13/pflag"
	"time"
)

var (
	ErrDiscoveryDomainRequired  = errors.New("discovery domain is required")
	ErrDiscoveryServiceRequired = errors.New("discovery service is required")
	ErrRootCARequired           = errors.New("root ca is required")
	ErrClientCertRequired       = errors.New("client cert is required")
	ErrClientKeyRequired        = errors.New("client key is required")
)

const (
	DefaultDisabled = false
)

type Config struct {
	Disabled         bool   `yaml:"disabled"`
	DiscoveryDomain  string `yaml:"discovery_domain"`
	DiscoveryService string `yaml:"discovery_service"`
	RootCA           string `yaml:"root_ca"`
	ClientCert       string `yaml:"client_cert"`
	ClientKey        string `yaml:"client_key"`
}

func New() *Config {
	return &Config{
		Disabled: DefaultDisabled,
	}
}

func (c *Config) Validate() error {
	if !c.Disabled {
		if c.DiscoveryDomain == "" {
			return ErrDiscoveryDomainRequired
		}

		if c.DiscoveryService == "" {
			return ErrDiscoveryServiceRequired
		}

		if c.RootCA == "" {
			return ErrRootCARequired
		}

		if c.ClientCert == "" {
			return ErrClientCertRequired
		}

		if c.ClientKey == "" {
			return ErrClientKeyRequired
		}
	}
	return nil
}

func (c *Config) RootPersistentFlags(flags *pflag.FlagSet) {
	flags.StringVar(&c.DiscoveryDomain, "etcd-discovery-domain", "", "The etcd discovery domain")
	flags.StringVar(&c.DiscoveryService, "etcd-discovery-service", "", "The etcd discovery service")
	flags.StringVar(&c.RootCA, "etcd-root-ca", "", "The etcd root ca")
	flags.StringVar(&c.ClientCert, "etcd-client-cert", "", "The etcd client cert")
	flags.StringVar(&c.ClientKey, "etcd-client-key", "", "The etcd client key")
}

func (c *Config) GenerateOptions(logName string) (*etcd.Options, error) {
	tlsConfig, err := tlsconfig.New(c.RootCA, c.ClientCert, c.ClientKey, time.Hour)
	if err != nil {
		return nil, fmt.Errorf("failed to create tls config: %w", err)
	}

	return &etcd.Options{
		LogName:     logName,
		Disabled:    c.Disabled,
		SrvDomain:   c.DiscoveryDomain,
		ServiceName: c.DiscoveryService,
		TLS:         tlsConfig,
	}, nil
}
