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
	"etcd/pkg/etcd"
	"etcd/pkg/tlsconfig"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"time"
)

var (
	ErrEtcdDiscoveryDomainRequired  = errors.New("etcd discovery domain is required")
	ErrEtcdDiscoveryServiceRequired = errors.New("etcd discovery service is required")
	ErrEtcdRootCARequired           = errors.New("etcd root ca is required")
	ErrEtcdClientCertRequired       = errors.New("etcd client cert is required")
	ErrEtcdClientKeyRequired        = errors.New("etcd client key is required")
)

type Config struct {
	EtcdDiscoveryDomain  string `yaml:"etcd_discovery_domain"`
	EtcdDiscoveryService string `yaml:"etcd_discovery_service"`
	EtcdRootCA           string `yaml:"etcd_root_ca"`
	EtcdClientCert       string `yaml:"etcd_client_cert"`
	EtcdClientKey        string `yaml:"etcd_client_key"`
}

func New() *Config {
	return new(Config)
}

func (c *Config) Validate() error {
	if c.EtcdDiscoveryDomain == "" {
		return ErrEtcdDiscoveryDomainRequired
	}

	if c.EtcdDiscoveryService == "" {
		return ErrEtcdDiscoveryServiceRequired
	}

	if c.EtcdRootCA == "" {
		return ErrEtcdRootCARequired
	}

	if c.EtcdClientCert == "" {
		return ErrEtcdClientCertRequired
	}

	if c.EtcdClientKey == "" {
		return ErrEtcdClientKeyRequired
	}

	return nil
}

func (c *Config) RootPersistentFlags(flags *pflag.FlagSet) {
	flags.StringVar(&c.EtcdDiscoveryDomain, "etcd-discovery-domain", "", "The etcd discovery domain")
	flags.StringVar(&c.EtcdDiscoveryService, "etcd-discovery-service", "", "The etcd discovery service")
	flags.StringVar(&c.EtcdRootCA, "etcd-root-ca", "", "The etcd root ca")
	flags.StringVar(&c.EtcdClientCert, "etcd-client-cert", "", "The etcd client cert")
	flags.StringVar(&c.EtcdClientKey, "etcd-client-key", "", "The etcd client key")
}

func (c *Config) GlobalRequiredFlags(cmd *cobra.Command) error {
	err := cmd.MarkFlagRequired("etcd-discovery-domain")
	if err != nil {
		return err
	}

	err = cmd.MarkFlagRequired("etcd-discovery-service")
	if err != nil {
		return err
	}

	err = cmd.MarkFlagRequired("etcd-root-ca")
	if err != nil {
		return err
	}

	err = cmd.MarkFlagRequired("etcd-client-cert")
	if err != nil {
		return err
	}

	err = cmd.MarkFlagRequired("etcd-client-key")
	if err != nil {
		return err
	}

	return nil
}

func (c *Config) GenerateEtcdOptions(projectName string) (*etcd.Options, error) {
	tlsConfig, err := tlsconfig.New(c.EtcdRootCA, c.EtcdClientCert, c.EtcdClientKey, time.Hour)
	if err != nil {
		return nil, fmt.Errorf("failed to create tls config: %w", err)
	}

	return &etcd.Options{
		Project:     projectName,
		SrvDomain:   c.EtcdDiscoveryDomain,
		ServiceName: c.EtcdDiscoveryService,
		TLS:         tlsConfig,
	}, nil
}
