// Package sdk provides a Go client for the kver distributed KV store.
// Uygulamalar bu paketi import ederek cluster'a bağlanabilir.
package sdk

import (
	"context"
	"fmt"
	"sync"
	"time"

	kvpb "github.com/emin/kver/proto/kv/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// rpcTimeout, her gRPC çağrısı için maksimum bekleme süresidir.
// Ölü node'a bağlanıldığında OS TCP timeout'u beklenmez; hızla sonraki node'a geçilir.
const rpcTimeout = 2 * time.Second

// Client, kver cluster'ına bağlanan Go SDK istemcisidir.
// Birden fazla node adresini tutarak leader discovery yapar.
type Client struct {
	mu     sync.RWMutex
	nodes  []string // gRPC adresleri
	leader string   // bilinen leader adresi

	connMu sync.Mutex
	conns  map[string]*grpc.ClientConn
}

// NewClient, verilen node adresleriyle yeni bir Client oluşturur.
func NewClient(nodes []string) *Client {
	return &Client{
		nodes: nodes,
		conns: make(map[string]*grpc.ClientConn),
	}
}

// Close, client kaynaklarını serbest bırakır.
func (c *Client) Close() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	for addr, conn := range c.conns {
		conn.Close()
		delete(c.conns, addr)
	}
	return nil
}

// getConn, belirtilen adres için cached gRPC bağlantısı döndürür.
func (c *Client) getConn(addr string) (*grpc.ClientConn, error) {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}

	backoffCfg := backoff.DefaultConfig
	backoffCfg.MaxDelay = 50 * time.Millisecond

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoffCfg,
			MinConnectTimeout: 500 * time.Millisecond,
		}),
	)
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	return conn, nil
}

// doWrite, yazma operasyonunu leader'a gönderir.
// Her deneme için rpcTimeout süreli bir context oluşturur.
// "not leader" veya geçici ağ hatalarında Exponential Backoff ile tekrar dener.
func (c *Client) doWrite(fn func(context.Context, kvpb.KVServiceClient) error) error {
	const maxRetries = 4
	baseDelay := 50 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		c.mu.RLock()
		leader := c.leader
		allNodes := c.nodes
		c.mu.RUnlock()

		// Önce bilinen leader'ı, sonra diğerlerini dene
		candidates := make([]string, 0, len(allNodes)+1)
		if leader != "" {
			candidates = append(candidates, leader)
		}
		for _, n := range allNodes {
			if n != leader {
				candidates = append(candidates, n)
			}
		}

		var lastErr error
		success := false

		for _, addr := range candidates {
			conn, err := c.getConn(addr)
			if err != nil {
				lastErr = err
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
			client := kvpb.NewKVServiceClient(conn)
			err = fn(ctx, client)
			cancel()

			if err != nil {
				lastErr = err
				st, ok := status.FromError(err)
				if ok && (st.Code() == codes.FailedPrecondition || st.Code() == codes.Unavailable || st.Code() == codes.DeadlineExceeded || st.Code() == codes.Canceled) {
					// not leader, node ulaşılamaz veya zaman aşımı — sonrakini dene
					continue
				}
				// Geri dönülemeyen kritik bir hata
				return err
			}
			// Başarılı — leader'ı güncelle
			c.mu.Lock()
			c.leader = addr
			c.mu.Unlock()
			success = true
			break
		}

		if success {
			return nil
		}

		// Tüm adaylar başarısız olduysa backoff uygula ve tekrar dene
		if attempt < maxRetries-1 {
			time.Sleep(baseDelay)
			baseDelay *= 2
		} else if lastErr != nil {
			return fmt.Errorf("no leader found in cluster after retries, last error: %v", lastErr)
		}
	}
	return fmt.Errorf("no leader found in cluster")
}

// doRead, okuma operasyonunu leader'a gönderir (linearizable okuma için).
func (c *Client) doRead(fn func(context.Context, kvpb.KVServiceClient) error) error {
	return c.doWrite(fn)
}

// ─── String Operations ────────────────────────────────────────────────────────

// Set, bir anahtara değer atar.
func (c *Client) Set(key, value string, ttl time.Duration) error {
	ttlMs := int64(ttl / time.Millisecond)
	return c.doWrite(func(ctx context.Context, client kvpb.KVServiceClient) error {
		_, err := client.Set(ctx, &kvpb.SetRequest{
			Key:   key,
			Value: value,
			TtlMs: ttlMs,
		})
		return err
	})
}

// Get, bir anahtarın değerini okur.
func (c *Client) Get(key string) (string, error) {
	var result string
	err := c.doRead(func(ctx context.Context, client kvpb.KVServiceClient) error {
		resp, err := client.Get(ctx, &kvpb.GetRequest{Key: key})
		if err != nil {
			return err
		}
		result = resp.Value
		return nil
	})
	return result, err
}

// Delete, bir anahtarı siler.
func (c *Client) Delete(key string) error {
	return c.doWrite(func(ctx context.Context, client kvpb.KVServiceClient) error {
		_, err := client.Delete(ctx, &kvpb.DeleteRequest{Key: key})
		return err
	})
}

// Incr, bir sayısal değeri 1 artırır.
func (c *Client) Incr(key string) (int64, error) {
	var result int64
	err := c.doWrite(func(ctx context.Context, client kvpb.KVServiceClient) error {
		resp, err := client.Incr(ctx, &kvpb.IncrRequest{Key: key})
		if err != nil {
			return err
		}
		result = resp.NewValue
		return nil
	})
	return result, err
}

// Decr, bir sayısal değeri 1 azaltır.
func (c *Client) Decr(key string) (int64, error) {
	var result int64
	err := c.doWrite(func(ctx context.Context, client kvpb.KVServiceClient) error {
		resp, err := client.Decr(ctx, &kvpb.DecrRequest{Key: key})
		if err != nil {
			return err
		}
		result = resp.NewValue
		return nil
	})
	return result, err
}

// ─── Hash Operations ──────────────────────────────────────────────────────────

// HSet, hash alanına değer atar.
func (c *Client) HSet(key, field, value string) error {
	return c.doWrite(func(ctx context.Context, client kvpb.KVServiceClient) error {
		_, err := client.HSet(ctx, &kvpb.HSetRequest{
			Key: key, Field: field, Value: value,
		})
		return err
	})
}

// HDelete, hash'ten bir field siler.
func (c *Client) HDelete(key, field string) error {
	return c.doWrite(func(ctx context.Context, client kvpb.KVServiceClient) error {
		_, err := client.HDelete(ctx, &kvpb.HDeleteRequest{
			Key: key, Field: field,
		})
		return err
	})
}

// HGet, bir hash alanının değerini okur.
func (c *Client) HGet(key, field string) (string, error) {
	var result string
	err := c.doRead(func(ctx context.Context, client kvpb.KVServiceClient) error {
		resp, err := client.HGet(ctx, &kvpb.HGetRequest{
			Key: key, Field: field,
		})
		if err != nil {
			return err
		}
		result = resp.Value
		return nil
	})
	return result, err
}

// HGetAll, tüm hash alanlarını döndürür.
func (c *Client) HGetAll(key string) (map[string]string, error) {
	var result map[string]string
	err := c.doRead(func(ctx context.Context, client kvpb.KVServiceClient) error {
		resp, err := client.HGetAll(ctx, &kvpb.HGetAllRequest{Key: key})
		if err != nil {
			return err
		}
		result = resp.Fields
		return nil
	})
	return result, err
}

// HExists, bir hash alanının var olup olmadığını kontrol eder.
func (c *Client) HExists(key, field string) (bool, error) {
	var result bool
	err := c.doRead(func(ctx context.Context, client kvpb.KVServiceClient) error {
		resp, err := client.HExists(ctx, &kvpb.HExistsRequest{
			Key: key, Field: field,
		})
		if err != nil {
			return err
		}
		result = resp.Exists
		return nil
	})
	return result, err
}

// ─── List Operations ──────────────────────────────────────────────────────────

// LPush, listenin başına değer(ler) ekler.
func (c *Client) LPush(key string, values ...string) (int64, error) {
	var result int64
	err := c.doWrite(func(ctx context.Context, client kvpb.KVServiceClient) error {
		resp, err := client.LPush(ctx, &kvpb.LPushRequest{
			Key: key, Values: values,
		})
		if err != nil {
			return err
		}
		result = resp.Count
		return nil
	})
	return result, err
}

// RPush, listenin sonuna değer(ler) ekler.
func (c *Client) RPush(key string, values ...string) (int64, error) {
	var result int64
	err := c.doWrite(func(ctx context.Context, client kvpb.KVServiceClient) error {
		resp, err := client.RPush(ctx, &kvpb.RPushRequest{
			Key: key, Values: values,
		})
		if err != nil {
			return err
		}
		result = resp.Count
		return nil
	})
	return result, err
}

// LPop, listenin başından bir eleman çıkarır.
func (c *Client) LPop(key string) (string, error) {
	var result string
	err := c.doWrite(func(ctx context.Context, client kvpb.KVServiceClient) error {
		resp, err := client.LPop(ctx, &kvpb.LPopRequest{Key: key})
		if err != nil {
			return err
		}
		result = resp.Value
		return nil
	})
	return result, err
}

// LRange, liste aralığını döndürür.
func (c *Client) LRange(key string, start, stop int) ([]string, error) {
	var result []string
	err := c.doRead(func(ctx context.Context, client kvpb.KVServiceClient) error {
		resp, err := client.LRange(ctx, &kvpb.LRangeRequest{
			Key: key, Start: int64(start), Stop: int64(stop),
		})
		if err != nil {
			return err
		}
		result = resp.Values
		return nil
	})
	return result, err
}

// RPop, listenin sonundan bir eleman çıkarır.
func (c *Client) RPop(key string) (string, error) {
	var result string
	err := c.doWrite(func(ctx context.Context, client kvpb.KVServiceClient) error {
		resp, err := client.RPop(ctx, &kvpb.RPopRequest{Key: key})
		if err != nil {
			return err
		}
		result = resp.Value
		return nil
	})
	return result, err
}

// LLen, listenin uzunluğunu döndürür.
func (c *Client) LLen(key string) (int64, error) {
	var result int64
	err := c.doRead(func(ctx context.Context, client kvpb.KVServiceClient) error {
		resp, err := client.LLen(ctx, &kvpb.LLenRequest{Key: key})
		if err != nil {
			return err
		}
		result = resp.Count
		return nil
	})
	return result, err
}

// ─── Sorted Set Operations ────────────────────────────────────────────────────

// ZAdd, sorted set'e eleman ekler.
func (c *Client) ZAdd(key string, score float64, member string) error {
	return c.doWrite(func(ctx context.Context, client kvpb.KVServiceClient) error {
		_, err := client.ZAdd(ctx, &kvpb.ZAddRequest{
			Key: key, Score: score, Member: member,
		})
		return err
	})
}

// ZRange, sorted set aralığını döndürür.
func (c *Client) ZRange(key string, start, stop int) ([]string, error) {
	var result []string
	err := c.doRead(func(ctx context.Context, client kvpb.KVServiceClient) error {
		resp, err := client.ZRange(ctx, &kvpb.ZRangeRequest{
			Key: key, Start: int64(start), Stop: int64(stop),
		})
		if err != nil {
			return err
		}
		result = resp.Members
		return nil
	})
	return result, err
}

// ZScore, sorted set'teki elemanın skorunu döndürür.
func (c *Client) ZScore(key, member string) (float64, error) {
	var result float64
	err := c.doRead(func(ctx context.Context, client kvpb.KVServiceClient) error {
		resp, err := client.ZScore(ctx, &kvpb.ZScoreRequest{Key: key, Member: member})
		if err != nil {
			return err
		}
		result = resp.Score
		return nil
	})
	return result, err
}

// ZRank, sorted set'teki elemanın sırasını döndürür.
func (c *Client) ZRank(key, member string) (int64, error) {
	var result int64
	err := c.doRead(func(ctx context.Context, client kvpb.KVServiceClient) error {
		resp, err := client.ZRank(ctx, &kvpb.ZRankRequest{Key: key, Member: member})
		if err != nil {
			return err
		}
		result = resp.Rank
		return nil
	})
	return result, err
}

// ZRevRange, sorted set aralığını tersten döndürür.
func (c *Client) ZRevRange(key string, start, stop int) ([]string, error) {
	var result []string
	err := c.doRead(func(ctx context.Context, client kvpb.KVServiceClient) error {
		resp, err := client.ZRevRange(ctx, &kvpb.ZRevRangeRequest{
			Key: key, Start: int64(start), Stop: int64(stop),
		})
		if err != nil {
			return err
		}
		result = resp.Members
		return nil
	})
	return result, err
}

// ZRem, sorted set'ten eleman çıkarır.
func (c *Client) ZRem(key, member string) error {
	return c.doWrite(func(ctx context.Context, client kvpb.KVServiceClient) error {
		_, err := client.ZRem(ctx, &kvpb.ZRemRequest{Key: key, Member: member})
		return err
	})
}

// ─── Cluster Management ──────────────────────────────────────────────────────

// ClusterStatus, her node'un erişilebilirlik durumunu raporlar (UP/DOWN).
//
// Not: Handler tüm okuma/yazma isteklerini şeffafça lider'e forward ettiğinden
// mevcut RPC'lerle leader/follower ayrımı yapılamaz. Leader bilgisi için
// sunucu loglarındaki "[nodeX] became leader" satırına bakın.
func (c *Client) ClusterStatus() (string, error) {
	type nodeStatus struct {
		addr   string
		up     bool
		nodeId string
		state  string
		term   uint64
	}

	statuses := make([]nodeStatus, len(c.nodes))
	var wg sync.WaitGroup
	for i, addr := range c.nodes {
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			conn, err := c.getConn(addr)
			if err != nil {
				statuses[i] = nodeStatus{addr: addr, up: false}
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			cl := kvpb.NewKVServiceClient(conn)
			resp, err := cl.Ping(ctx, &kvpb.PingRequest{})

			if err == nil {
				statuses[i] = nodeStatus{
					addr:   addr,
					up:     true,
					nodeId: resp.NodeId,
					state:  resp.State,
					term:   resp.Term,
				}
			} else {
				fmt.Printf("Ping to %s failed: %v\n", addr, err)
				statuses[i] = nodeStatus{addr: addr, up: false}
			}
		}(i, addr)
	}
	wg.Wait()

	result := fmt.Sprintf("Cluster Status (%d nodes)\n", len(c.nodes))
	result += "────────────────────────────────────────────────────────────\n"
	result += fmt.Sprintf("  %-3s  %-15s %-20s %-10s %s\n", "ST", "NODE ID", "ADDRESS", "STATE", "TERM")
	result += "────────────────────────────────────────────────────────────\n"
	anyUp := false
	for _, s := range statuses {
		icon := "✗"
		if s.up {
			icon = "✓"
			anyUp = true
			result += fmt.Sprintf("  %-3s  %-15s %-20s %-10s %d\n", icon, s.nodeId, s.addr, s.state, s.term)
		} else {
			result += fmt.Sprintf("  %-3s  %-15s %-20s %-10s %s\n", icon, "???", s.addr, "DOWN", "-")
		}
	}

	if !anyUp {
		fmt.Print(result)
		return result, fmt.Errorf("cluster unreachable")
	}
	return result, nil
}


// AddNode, cluster'a yeni bir node ekler.
func (c *Client) AddNode(nodeID, addr string) error {
	return c.doWrite(func(ctx context.Context, client kvpb.KVServiceClient) error {
		_, err := client.AddNode(ctx, &kvpb.AddNodeRequest{
			NodeId:  nodeID,
			Address: addr,
		})
		return err
	})
}

// RemoveNode, cluster'dan bir node çıkarır.
func (c *Client) RemoveNode(nodeID string) error {
	return c.doWrite(func(ctx context.Context, client kvpb.KVServiceClient) error {
		_, err := client.RemoveNode(ctx, &kvpb.RemoveNodeRequest{
			NodeId: nodeID,
		})
		return err
	})
}
