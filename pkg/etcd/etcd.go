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

package etcd

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/rs/zerolog"
	"go.etcd.io/etcd/client/pkg/v3/srv"
	"go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"strings"
	"sync"
	"time"
)

var (
	ErrSetDelete           = errors.New("set-delete failed")
	ErrInvalidBatchRequest = errors.New("invalid batch request")
	ErrBatchRequestFailed  = errors.New("batch request failed")
)

const (
	DefaultTTL = 10
)

// Client is a wrapper for the etcd client
// that exposes the functionality needed by Architect
type Client struct {
	logger    *zerolog.Logger
	client    *clientv3.Client
	lease     *clientv3.LeaseGrantResponse
	keepalive <-chan *clientv3.LeaseKeepAliveResponse
	context   context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

func New(project string, srvDomain string, serviceName string, tls *tls.Config, logger *zerolog.Logger, zapLogger *zap.Logger) (*Client, error) {
	l := logger.With().Str(project, "ETCD").Logger()
	l.Debug().Msgf("connecting to etcd with srv-domain '%s'", srvDomain)

	srvs, err := srv.GetClient("etcd-client", srvDomain, serviceName)
	if err != nil {
		return nil, err
	}

	var endpoints []string
	for _, ep := range srvs.Endpoints {
		if strings.HasPrefix(ep, "http://") {
			l.Warn().Msgf("etcd endpoint '%s' is not using TLS, ignoring", ep)
			continue
		}
		endpoints = append(endpoints, ep)
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:            endpoints,
		AutoSyncInterval:     time.Second * 10,
		DialTimeout:          time.Second * 5,
		DialKeepAliveTime:    time.Second * 5,
		DialKeepAliveTimeout: time.Second * 5,
		TLS:                  tls,
		RejectOldCluster:     true,
		Logger:               zapLogger.With(zap.String(project, "ETCD")),
	})
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*DefaultTTL)
	lease, err := client.Grant(ctx, DefaultTTL)
	cancel()
	if err != nil {
		return nil, err
	}

	ctx, cancel = context.WithCancel(context.Background())
	keepalive, err := client.KeepAlive(ctx, lease.ID)
	if err != nil {
		cancel()
		return nil, err
	}

	e := &Client{
		logger:    &l,
		client:    client,
		lease:     lease,
		keepalive: keepalive,
		context:   ctx,
		cancel:    cancel,
	}

	e.wg.Add(1)
	go e.consumeKeepAlive()

	l.Debug().Msg("etcd client started successfully")

	return e, nil
}

func (e *Client) Close() error {
	e.logger.Debug().Msg("closing etcd client")
	e.cancel()
	defer e.wg.Wait()
	return e.client.Close()
}

func (e *Client) Set(ctx context.Context, key string, value string) (*clientv3.PutResponse, error) {
	return e.client.Put(ctx, key, value)
}

func (e *Client) SetDelete(ctx context.Context, key string, value string, delete string) error {
	resp, err := e.client.Txn(ctx).Then(clientv3.OpPut(key, value), clientv3.OpDelete(delete)).Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return ErrSetDelete
	}
	return nil
}

func (e *Client) SetKeepAlive(ctx context.Context, key string, value string) (*clientv3.PutResponse, error) {
	return e.client.Put(ctx, key, value, clientv3.WithLease(e.lease.ID))
}

func (e *Client) SetExpiry(ctx context.Context, key string, value string, ttl time.Duration) (*clientv3.PutResponse, error) {
	lease, err := e.client.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return nil, err
	}
	resp, err := e.client.Put(ctx, key, value, clientv3.WithLease(lease.ID))
	if err != nil {
		_, _ = e.client.Revoke(ctx, lease.ID)
		return nil, err
	}
	return resp, nil
}

func (e *Client) Get(ctx context.Context, key string) (*clientv3.GetResponse, error) {
	return e.client.Get(ctx, key)
}

func (e *Client) GetLimit(ctx context.Context, key string, limit int64) (*clientv3.GetResponse, error) {
	return e.client.Get(ctx, key, clientv3.WithLimit(limit))
}

func (e *Client) GetPrefix(ctx context.Context, prefix string) (*clientv3.GetResponse, error) {
	return e.client.Get(ctx, prefix, clientv3.WithPrefix())
}

func (e *Client) GetBatch(ctx context.Context, keys ...string) (*clientv3.TxnResponse, error) {
	if len(keys) == 0 {
		return nil, ErrInvalidBatchRequest
	}
	txn := e.client.Txn(ctx)
	ops := make([]clientv3.Op, 0, len(keys))
	for _, key := range keys {
		ops = append(ops, clientv3.OpGet(key, clientv3.WithLimit(1)))
	}
	txn = txn.Then(ops...)

	resp, err := txn.Commit()
	if err != nil {
		return nil, err
	}

	if !resp.Succeeded {
		return nil, ErrBatchRequestFailed
	}

	return resp, nil
}

func (e *Client) Delete(ctx context.Context, key string) (*clientv3.DeleteResponse, error) {
	return e.client.Delete(ctx, key)
}

func (e *Client) Watch(ctx context.Context, key string) clientv3.WatchChan {
	return e.client.Watch(ctx, key)
}

func (e *Client) WatchPrefix(ctx context.Context, prefix string) clientv3.WatchChan {
	return e.client.Watch(ctx, prefix, clientv3.WithPrefix())
}

func (e *Client) consumeKeepAlive() {
	defer e.wg.Done()
	for {
		select {
		case r, ok := <-e.keepalive:
			if !ok {
				e.logger.Warn().Msg("keepalive channel closed")
				return
			}
			e.logger.Trace().Msgf("keepalive received for ID %d", r.ID)
		case <-e.context.Done():
			e.logger.Debug().Msg("closing keepalive consumer")
			return
		}
	}
}
