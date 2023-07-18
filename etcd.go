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
	"errors"
	"github.com/loopholelabs/etcd/pkg/tlsconfig"
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
	ErrSetIfNotExist       = errors.New("set-if-not-exists failed")
	ErrInvalidBatchRequest = errors.New("invalid batch request")
	ErrBatchRequestFailed  = errors.New("batch request failed")
)

const (
	DefaultTTL = 10
)

type Options struct {
	LogName     string
	SrvDomain   string
	ServiceName string
	TLS         *tlsconfig.TLSConfig
}

// ETCD is a wrapper for the etcd client
type ETCD struct {
	logger  *zerolog.Logger
	options *Options

	client    *clientv3.Client
	lease     *clientv3.LeaseGrantResponse
	keepalive <-chan *clientv3.LeaseKeepAliveResponse

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(options *Options, logger *zerolog.Logger, zapLogger *zap.Logger) (*ETCD, error) {
	l := logger.With().Str(options.LogName, "ETCD").Logger()
	l.Debug().Msgf("connecting to etcd with srv-domain '%s' and service name '%s'", options.SrvDomain, options.ServiceName)

	srvs, err := srv.GetClient("etcd-client", options.SrvDomain, options.ServiceName)
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
		AutoSyncInterval:     time.Second * DefaultTTL,
		DialTimeout:          time.Second * DefaultTTL,
		DialKeepAliveTime:    time.Second * DefaultTTL,
		DialKeepAliveTimeout: time.Second * DefaultTTL,
		TLS:                  options.TLS.Config(),
		RejectOldCluster:     true,
		Logger:               zapLogger.With(zap.String(options.LogName, "ETCD")),
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

	e := &ETCD{
		logger:    &l,
		options:   options,
		client:    client,
		lease:     lease,
		keepalive: keepalive,
		ctx:       ctx,
		cancel:    cancel,
	}

	e.wg.Add(1)
	go e.consumeKeepAlive()

	l.Debug().Msg("etcd client started successfully")

	return e, nil
}

func (e *ETCD) Close() error {
	e.logger.Debug().Msg("closing etcd client")
	e.cancel()
	if e.options.TLS != nil {
		e.options.TLS.Stop()
	}
	defer e.wg.Wait()
	err := e.client.Close()
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func (e *ETCD) Set(ctx context.Context, key string, value string) (*clientv3.PutResponse, error) {
	return e.client.Put(ctx, key, value)
}

func (e *ETCD) SetDelete(ctx context.Context, key string, value string, delete string) error {
	resp, err := e.client.Txn(ctx).Then(clientv3.OpPut(key, value), clientv3.OpDelete(delete)).Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return ErrSetDelete
	}
	return nil
}

func (e *ETCD) SetKeepAlive(ctx context.Context, key string, value string) (*clientv3.PutResponse, error) {
	return e.client.Put(ctx, key, value, clientv3.WithLease(e.lease.ID))
}

func (e *ETCD) SetExpiry(ctx context.Context, key string, value string, ttl time.Duration) (*clientv3.PutResponse, error) {
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

func (e *ETCD) SetIfNotExist(ctx context.Context, key string, value string) (*clientv3.TxnResponse, error) {
	resp, err := e.client.Txn(ctx).If(clientv3.Compare(clientv3.Version(key), "=", 0)).Then(clientv3.OpPut(key, value)).Commit()
	if err != nil {
		return nil, err
	}
	if !resp.Succeeded {
		return nil, ErrSetIfNotExist
	}
	return resp, nil
}

func (e *ETCD) SetIfNotExistExpiry(ctx context.Context, key string, value string, ttl time.Duration) (*clientv3.TxnResponse, error) {
	lease, err := e.client.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return nil, err
	}
	resp, err := e.client.Txn(ctx).If(clientv3.Compare(clientv3.Version(key), "=", 0)).Then(clientv3.OpPut(key, value, clientv3.WithLease(lease.ID))).Commit()
	if err != nil {
		_, _ = e.client.Revoke(ctx, lease.ID)
		return nil, err
	}
	if !resp.Succeeded {
		return nil, ErrSetIfNotExist
	}
	return resp, nil
}

func (e *ETCD) Get(ctx context.Context, key string) (*clientv3.GetResponse, error) {
	return e.client.Get(ctx, key)
}

func (e *ETCD) GetLimit(ctx context.Context, key string, limit int64) (*clientv3.GetResponse, error) {
	return e.client.Get(ctx, key, clientv3.WithLimit(limit))
}

func (e *ETCD) GetPrefix(ctx context.Context, prefix string) (*clientv3.GetResponse, error) {
	return e.client.Get(ctx, prefix, clientv3.WithPrefix())
}

func (e *ETCD) GetBatch(ctx context.Context, keys ...string) (*clientv3.TxnResponse, error) {
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

func (e *ETCD) Delete(ctx context.Context, key string) (*clientv3.DeleteResponse, error) {
	return e.client.Delete(ctx, key)
}

func (e *ETCD) Watch(ctx context.Context, key string) clientv3.WatchChan {
	return e.client.Watch(ctx, key)
}

func (e *ETCD) WatchPrefix(ctx context.Context, prefix string) clientv3.WatchChan {
	return e.client.Watch(ctx, prefix, clientv3.WithPrefix())
}

func (e *ETCD) consumeKeepAlive() {
	defer e.wg.Done()
	for {
		select {
		case r, ok := <-e.keepalive:
			if !ok {
				e.logger.Warn().Msg("keepalive channel closed")
				return
			}
			e.logger.Trace().Msgf("keepalive received for ID %d", r.ID)
		case <-e.ctx.Done():
			e.logger.Debug().Msg("closing keepalive consumer")
			return
		}
	}
}
