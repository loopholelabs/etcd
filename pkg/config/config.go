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
	"github.com/loopholelabs/etcd"
	"github.com/spf13/pflag"
)

var (
	ErrDiscoveryDomainRequired  = errors.New("discovery domain is required")
	ErrDiscoveryServiceRequired = errors.New("discovery service is required")
)

const (
	DefaultDisabled = false
)

type Config struct {
	Disabled         bool   `mapstructure:"disabled"`
	DiscoveryDomain  string `mapstructure:"discovery_domain"`
	DiscoveryService string `mapstructure:"discovery_service"`
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

	}
	return nil
}

func (c *Config) RootPersistentFlags(flags *pflag.FlagSet) {
	flags.BoolVar(&c.Disabled, "etcd-disabled", DefaultDisabled, "Disable etcd")
	flags.StringVar(&c.DiscoveryDomain, "etcd-discovery-domain", "", "The etcd discovery domain")
	flags.StringVar(&c.DiscoveryService, "etcd-discovery-service", "", "The etcd discovery service")
}

func (c *Config) GenerateOptions(logName string) (*etcd.Options, error) {
	return &etcd.Options{
		LogName:     logName,
		Disabled:    c.Disabled,
		SrvDomain:   c.DiscoveryDomain,
		ServiceName: c.DiscoveryService,
	}, nil
}
