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

package tlsconfig

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
)

// TLSConfig contains a set of TLS configuration options and is able to reload a certificate
// and the Root CAs associated with it on a given interval, or on demand.
type TLSConfig struct {
	rootCAPath     string
	clientCertPath string
	clientKeyPath  string
	interval       time.Duration

	cert *tls.Certificate

	config *tls.Config
	mu     sync.RWMutex
	err    error
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func New(rootCA string, clientCert string, clientKey string, interval time.Duration) (*TLSConfig, error) {
	t := &TLSConfig{
		rootCAPath:     rootCA,
		clientCertPath: clientCert,
		clientKeyPath:  clientKey,
		interval:       interval,
	}

	t.ctx, t.cancel = context.WithCancel(context.Background())

	rootPool := x509.NewCertPool()
	rootCAPEM, err := os.ReadFile(rootCA)
	if err != nil {
		return nil, fmt.Errorf("failed to read root CA: %w", err)
	}
	rootPool.AppendCertsFromPEM(rootCAPEM)

	t.config = &tls.Config{
		RootCAs: rootPool,
		GetClientCertificate: func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			t.mu.RLock()
			defer t.mu.RUnlock()
			return t.cert, t.err
		},
	}

	cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate and key: %w", err)
	}

	t.cert = &cert

	t.wg.Add(1)
	go t.rotate()

	return t, nil
}

func (t *TLSConfig) Config() *tls.Config {
	return t.config
}

func (t *TLSConfig) Stop() {
	t.cancel()
	t.wg.Wait()
}

func (t *TLSConfig) rotate() {
	defer t.wg.Done()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-time.After(t.interval + time.Second*(time.Duration(rand.Intn(300)))):
			cert, err := tls.LoadX509KeyPair(t.clientCertPath, t.clientKeyPath)
			if err != nil {
				t.mu.Lock()
				t.err = fmt.Errorf("failed to load client certificate and key: %w", err)
				t.mu.Unlock()
				continue
			}

			t.mu.Lock()
			t.cert = &cert
			t.err = nil
			t.mu.Unlock()
		}
	}
}
