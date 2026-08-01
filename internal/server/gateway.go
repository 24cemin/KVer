// Package server — gateway.go
// HTTP/1.1 gateway: REST endpoint'lerini gRPC'ye çevirir.
// Tüm veri tiplerini (String, Hash, List, Sorted Set) destekler.
package server

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/emin/kver/pkg/sdk"
)

// Gateway, REST-to-gRPC dönüşümünü sağlayan HTTP handler'dır.
type Gateway struct {
	addr   string
	client *sdk.Client
	server *http.Server
}

// NewGateway, yeni bir Gateway oluşturur.
func NewGateway(addr string, client *sdk.Client) *Gateway {
	g := &Gateway{
		addr:   addr,
		client: client,
	}

	mux := http.NewServeMux()

	// --- String Operations ---
	mux.HandleFunc("POST /api/v1/string/set", g.handleSet)
	mux.HandleFunc("GET /api/v1/string/get", g.handleGet)
	mux.HandleFunc("DELETE /api/v1/string/delete", g.handleDelete)
	mux.HandleFunc("POST /api/v1/string/incr", g.handleIncr)
	mux.HandleFunc("POST /api/v1/string/decr", g.handleDecr)

	// --- Hash Operations ---
	mux.HandleFunc("POST /api/v1/hash/set", g.handleHSet)
	mux.HandleFunc("GET /api/v1/hash/get", g.handleHGet)
	mux.HandleFunc("GET /api/v1/hash/getall", g.handleHGetAll)

	// --- List Operations ---
	mux.HandleFunc("POST /api/v1/list/lpush", g.handleLPush)
	mux.HandleFunc("POST /api/v1/list/rpush", g.handleRPush)
	mux.HandleFunc("POST /api/v1/list/lpop", g.handleLPop)
	mux.HandleFunc("GET /api/v1/list/range", g.handleLRange)

	// --- Sorted Set Operations ---
	mux.HandleFunc("POST /api/v1/zset/add", g.handleZAdd)
	mux.HandleFunc("GET /api/v1/zset/range", g.handleZRange)

	// --- Cluster Management ---
	mux.HandleFunc("POST /api/v1/cluster/add-node", g.handleAddNode)
	mux.HandleFunc("POST /api/v1/cluster/remove-node", g.handleRemoveNode)

	// --- Health check ---
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("OK\n")); err != nil {
			log.Printf("failed to write health response: %v", err)
		}
	})

	g.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return g
}

// Start, HTTP gateway'i başlatır. Context iptal edildiğinde graceful shutdown yapar.
func (g *Gateway) Start(ctx context.Context) error {
	errCh := make(chan error, 1)

	go func() {
		log.Printf("HTTP Gateway listening on %s", g.addr)
		if err := g.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Println("Context cancelled, shutting down HTTP Gateway gracefully...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return g.server.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

// Stop, HTTP gateway'i durdurur. Start(ctx) zaten graceful shutdown yaptığı için opsiyonel.
func (g *Gateway) Stop() {
	if g.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := g.server.Shutdown(ctx); err != nil {
			log.Printf("failed to stop HTTP Gateway: %v", err)
		}
	}
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func (g *Gateway) respondError(w http.ResponseWriter, msg string, code int) {
	http.Error(w, msg, code)
}

func (g *Gateway) respondJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("failed to encode HTTP JSON response: %v", err)
	}
}

func (g *Gateway) respondSuccess(w http.ResponseWriter) {
	g.respondJSON(w, http.StatusOK, map[string]bool{"success": true})
}

type keyReq struct {
	Key string `json:"key"`
}

// ─── String Handlers ──────────────────────────────────────────────────────────

type setReq struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	TtlMs int64  `json:"ttl_ms"`
}

func (g *Gateway) handleSet(w http.ResponseWriter, r *http.Request) {
	var req setReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		g.respondError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Key == "" {
		g.respondError(w, "key is required", http.StatusBadRequest)
		return
	}

	err := g.client.Set(req.Key, req.Value, time.Duration(req.TtlMs)*time.Millisecond)
	if err != nil {
		g.respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	g.respondSuccess(w)
}

func (g *Gateway) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		g.respondError(w, "key is required", http.StatusBadRequest)
		return
	}

	val, err := g.client.Get(key)
	if err != nil {
		g.respondError(w, err.Error(), http.StatusNotFound)
		return
	}

	g.respondJSON(w, http.StatusOK, map[string]string{"value": val})
}

func (g *Gateway) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		g.respondError(w, "key is required", http.StatusBadRequest)
		return
	}

	err := g.client.Delete(key)
	if err != nil {
		g.respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	g.respondSuccess(w)
}

func (g *Gateway) handleIncr(w http.ResponseWriter, r *http.Request) {
	var req keyReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		g.respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	val, err := g.client.Incr(req.Key)
	if err != nil {
		g.respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	g.respondJSON(w, http.StatusOK, map[string]int64{"value": val})
}

func (g *Gateway) handleDecr(w http.ResponseWriter, r *http.Request) {
	var req keyReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		g.respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	val, err := g.client.Decr(req.Key)
	if err != nil {
		g.respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	g.respondJSON(w, http.StatusOK, map[string]int64{"value": val})
}

// ─── Hash Handlers ────────────────────────────────────────────────────────────

type hsetReq struct {
	Key   string `json:"key"`
	Field string `json:"field"`
	Value string `json:"value"`
}

func (g *Gateway) handleHSet(w http.ResponseWriter, r *http.Request) {
	var req hsetReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		g.respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := g.client.HSet(req.Key, req.Field, req.Value)
	if err != nil {
		g.respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	g.respondSuccess(w)
}

func (g *Gateway) handleHGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	field := r.URL.Query().Get("field")
	if key == "" || field == "" {
		g.respondError(w, "key and field are required", http.StatusBadRequest)
		return
	}

	val, err := g.client.HGet(key, field)
	if err != nil {
		g.respondError(w, err.Error(), http.StatusNotFound)
		return
	}

	g.respondJSON(w, http.StatusOK, map[string]string{"value": val})
}

func (g *Gateway) handleHGetAll(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		g.respondError(w, "key is required", http.StatusBadRequest)
		return
	}

	fields, err := g.client.HGetAll(key)
	if err != nil {
		g.respondError(w, err.Error(), http.StatusNotFound)
		return
	}

	g.respondJSON(w, http.StatusOK, map[string]map[string]string{"fields": fields})
}

// ─── List Handlers ────────────────────────────────────────────────────────────

type lpushReq struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
}

func (g *Gateway) handleLPush(w http.ResponseWriter, r *http.Request) {
	var req lpushReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		g.respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	count, err := g.client.LPush(req.Key, req.Values...)
	if err != nil {
		g.respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	g.respondJSON(w, http.StatusOK, map[string]int64{"count": count})
}

func (g *Gateway) handleRPush(w http.ResponseWriter, r *http.Request) {
	var req lpushReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		g.respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	count, err := g.client.RPush(req.Key, req.Values...)
	if err != nil {
		g.respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	g.respondJSON(w, http.StatusOK, map[string]int64{"count": count})
}

func (g *Gateway) handleLPop(w http.ResponseWriter, r *http.Request) {
	var req keyReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		g.respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	val, err := g.client.LPop(req.Key)
	if err != nil {
		g.respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	g.respondJSON(w, http.StatusOK, map[string]string{"value": val})
}

func (g *Gateway) handleLRange(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	startStr := r.URL.Query().Get("start")
	stopStr := r.URL.Query().Get("stop")

	if key == "" {
		g.respondError(w, "key is required", http.StatusBadRequest)
		return
	}

	start, _ := strconv.Atoi(startStr)
	stop, err := strconv.Atoi(stopStr)
	if err != nil {
		stop = -1 // default
	}

	vals, err := g.client.LRange(key, start, stop)
	if err != nil {
		g.respondError(w, err.Error(), http.StatusNotFound)
		return
	}

	g.respondJSON(w, http.StatusOK, map[string][]string{"values": vals})
}

// ─── Sorted Set Handlers ──────────────────────────────────────────────────────

type zaddReq struct {
	Key    string  `json:"key"`
	Score  float64 `json:"score"`
	Member string  `json:"member"`
}

func (g *Gateway) handleZAdd(w http.ResponseWriter, r *http.Request) {
	var req zaddReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		g.respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := g.client.ZAdd(req.Key, req.Score, req.Member)
	if err != nil {
		g.respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	g.respondSuccess(w)
}

func (g *Gateway) handleZRange(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	startStr := r.URL.Query().Get("start")
	stopStr := r.URL.Query().Get("stop")

	if key == "" {
		g.respondError(w, "key is required", http.StatusBadRequest)
		return
	}

	start, _ := strconv.Atoi(startStr)
	stop, err := strconv.Atoi(stopStr)
	if err != nil {
		stop = -1 // default
	}

	members, err := g.client.ZRange(key, start, stop)
	if err != nil {
		g.respondError(w, err.Error(), http.StatusNotFound)
		return
	}

	g.respondJSON(w, http.StatusOK, map[string][]string{"members": members})
}

// ─── Cluster Management ───────────────────────────────────────────────────────

func (g *Gateway) handleAddNode(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID  string `json:"node_id"`
		Address string `json:"address"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		g.respondError(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.NodeID == "" || req.Address == "" {
		g.respondError(w, "node_id and address are required", http.StatusBadRequest)
		return
	}
	if err := g.client.AddNode(req.NodeID, req.Address); err != nil {
		g.respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	g.respondSuccess(w)
}

func (g *Gateway) handleRemoveNode(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID string `json:"node_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		g.respondError(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.NodeID == "" {
		g.respondError(w, "node_id is required", http.StatusBadRequest)
		return
	}
	if err := g.client.RemoveNode(req.NodeID); err != nil {
		g.respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	g.respondSuccess(w)
}
