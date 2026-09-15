package main

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"legacyconnect/proxy"
)

//go:embed web/index.html
var indexHTML []byte

const (
	appName        = "LegacyConnect"
	defaultWebAddr = "127.0.0.1:8099"
)

type storedServer struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Address    string    `json:"address"`
	LastPlayed time.Time `json:"lastPlayed,omitempty"`
}

type config struct {
	Servers      []storedServer `json:"servers"`
	SelectedID   string         `json:"selectedId"`
	ProxyAddress string         `json:"proxyAddress"`
	Version      string         `json:"version"`
	Protocol     int            `json:"protocol"`
}

type app struct {
	mu         sync.RWMutex
	config     config
	configPath string
	proxy      *proxy.Server
	log        *slog.Logger
}

type stateResponse struct {
	Version  string       `json:"version"`
	Protocol int          `json:"protocol"`
	Proxy    proxy.Status `json:"proxy"`
	Config   config       `json:"config"`
}

func main() {
	var (
		targetFlag    = flag.String("target", "", "connect directly to this Bedrock server without opening the UI")
		listenFlag    = flag.String("listen", "", "local proxy address (default 127.0.0.1:19132)")
		webFlag       = flag.String("web", defaultWebAddr, "local web UI address")
		configFlag    = flag.String("config", "", "configuration file path")
		noBrowserFlag = flag.Bool("no-browser", false, "do not open the web UI in a browser")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if *targetFlag != "" {
		runHeadless(*targetFlag, *listenFlag, logger)
		return
	}

	configPath := *configFlag
	if configPath == "" {
		configPath = defaultConfigPath()
	}
	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if *listenFlag != "" {
		normalized, err := proxy.NormalizeListenAddress(*listenFlag)
		if err != nil {
			log.Fatal(err)
		}
		cfg.ProxyAddress = normalized
	}
	if cfg.ProxyAddress == "" {
		cfg.ProxyAddress = proxy.DefaultProxyAddress
	}
	if cfg.Version == "" {
		cfg.Version = proxy.GameVersion
	}
	if cfg.Protocol == 0 {
		cfg.Protocol = proxy.ProtocolVersion
	}

	app := &app{
		config:     cfg,
		configPath: configPath,
		proxy:      proxy.New(cfg.ProxyAddress, logger),
		log:        logger,
	}
	if err := app.proxy.SetProfile(cfg.Version, cfg.Protocol); err != nil {
		log.Fatal(err)
	}
	app.ensureSelectedTarget()

	uiURL := browserURL(*webFlag)
	if runningInstance(uiURL) {
		logger.Info("control panel is already running", "url", uiURL)
		if !*noBrowserFlag {
			openBrowser(uiURL)
		}
		return
	}

	server := &http.Server{
		Addr:              *webFlag,
		Handler:           app.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		if !*noBrowserFlag {
			time.Sleep(250 * time.Millisecond)
			openBrowser(uiURL)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	logger.Info("control panel listening", "url", "http://"+*webFlag)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}

	_ = app.proxy.Stop()
	app.mu.Lock()
	err = saveConfig(app.configPath, app.config)
	app.mu.Unlock()
	if err != nil {
		logger.Warn("save config", "error", err)
	}
}

func runHeadless(target, listenAddr string, logger *slog.Logger) {
	if listenAddr == "" {
		listenAddr = proxy.DefaultProxyAddress
	}
	relay := proxy.New(listenAddr, logger)
	if err := relay.Start(target); err != nil {
		log.Fatal(err)
	}
	status := relay.Status()
	logger.Info("proxy running", "listen", status.Listen, "target", status.Target)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	_ = relay.Stop()
}

func (a *app) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})
	mux.HandleFunc("GET /favicon.ico", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /api/state", a.handleState)
	mux.HandleFunc("POST /api/servers", a.handleAddServer)
	mux.HandleFunc("PUT /api/servers/{id}", a.handleEditServer)
	mux.HandleFunc("DELETE /api/servers/{id}", a.handleDeleteServer)
	mux.HandleFunc("POST /api/select", a.handleSelectServer)
	mux.HandleFunc("POST /api/ping", a.handlePing)
	mux.HandleFunc("POST /api/profile", a.handleProfile)
	mux.HandleFunc("POST /api/start", a.handleStart)
	mux.HandleFunc("POST /api/stop", a.handleStop)
	return mux
}

func (a *app) handleState(w http.ResponseWriter, _ *http.Request) {
	a.mu.RLock()
	cfg := cloneConfig(a.config)
	a.mu.RUnlock()
	status := a.proxy.Status()
	writeJSON(w, http.StatusOK, stateResponse{
		Version:  status.Version,
		Protocol: status.Protocol,
		Proxy:    status,
		Config:   cfg,
	})
}

func (a *app) handleAddServer(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name    string `json:"name"`
		Address string `json:"address"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	address, err := proxy.NormalizeAddress(body.Address)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	name := strings.TrimSpace(body.Name)
	if name == "" {
		name = address
	}

	a.mu.Lock()
	srv := storedServer{
		ID:      newID(),
		Name:    name,
		Address: address,
	}
	a.config.Servers = append(a.config.Servers, srv)
	if a.config.SelectedID == "" {
		a.config.SelectedID = srv.ID
	}
	err = saveConfig(a.configPath, a.config)
	selected := a.selectedTargetLocked()
	a.mu.Unlock()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if selected != "" {
		_ = a.proxy.SetTarget(selected)
	}
	writeJSON(w, http.StatusCreated, srv)
}

func (a *app) handleEditServer(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body struct {
		Name    string `json:"name"`
		Address string `json:"address"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	address, err := proxy.NormalizeAddress(body.Address)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	a.mu.Lock()
	index := a.serverIndexLocked(id)
	if index < 0 {
		a.mu.Unlock()
		writeError(w, http.StatusNotFound, errors.New("server not found"))
		return
	}
	a.config.Servers[index].Name = strings.TrimSpace(body.Name)
	if a.config.Servers[index].Name == "" {
		a.config.Servers[index].Name = address
	}
	a.config.Servers[index].Address = address
	err = saveConfig(a.configPath, a.config)
	selected := a.selectedTargetLocked()
	a.mu.Unlock()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if selected != "" {
		_ = a.proxy.SetTarget(selected)
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *app) handleDeleteServer(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	a.mu.Lock()
	index := a.serverIndexLocked(id)
	if index < 0 {
		a.mu.Unlock()
		writeError(w, http.StatusNotFound, errors.New("server not found"))
		return
	}
	a.config.Servers = append(a.config.Servers[:index], a.config.Servers[index+1:]...)
	if a.config.SelectedID == id {
		a.config.SelectedID = ""
		if len(a.config.Servers) > 0 {
			a.config.SelectedID = a.config.Servers[0].ID
		}
	}
	err := saveConfig(a.configPath, a.config)
	selected := a.selectedTargetLocked()
	a.mu.Unlock()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if selected != "" {
		_ = a.proxy.SetTarget(selected)
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *app) handleSelectServer(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID string `json:"id"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	a.mu.Lock()
	index := a.serverIndexLocked(body.ID)
	if index < 0 {
		a.mu.Unlock()
		writeError(w, http.StatusNotFound, errors.New("server not found"))
		return
	}
	a.config.SelectedID = body.ID
	a.config.Servers[index].LastPlayed = time.Now().UTC()
	err := saveConfig(a.configPath, a.config)
	target := a.config.Servers[index].Address
	a.mu.Unlock()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if err := a.proxy.SetTarget(target); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "target": target})
}

func (a *app) handlePing(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Address string `json:"address"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	expectedProtocol := a.proxy.Status().Protocol
	pong, err := proxy.Ping(body.Address, 4*time.Second, expectedProtocol)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, pong)
}

func (a *app) handleProfile(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Version  string `json:"version"`
		Protocol int    `json:"protocol"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := a.proxy.SetProfile(body.Version, body.Protocol); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	a.mu.Lock()
	a.config.Version = strings.TrimSpace(body.Version)
	a.config.Protocol = body.Protocol
	err := saveConfig(a.configPath, a.config)
	a.mu.Unlock()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, a.proxy.Status())
}

func (a *app) handleStart(w http.ResponseWriter, _ *http.Request) {
	a.mu.RLock()
	target := a.selectedTargetLocked()
	a.mu.RUnlock()
	if target == "" {
		writeError(w, http.StatusBadRequest, errors.New("select a server first"))
		return
	}
	if err := a.proxy.Start(target); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, a.proxy.Status())
}

func (a *app) handleStop(w http.ResponseWriter, _ *http.Request) {
	if err := a.proxy.Stop(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, a.proxy.Status())
}

func (a *app) selectedTargetLocked() string {
	index := a.serverIndexLocked(a.config.SelectedID)
	if index < 0 {
		return ""
	}
	return a.config.Servers[index].Address
}

func (a *app) serverIndexLocked(id string) int {
	for i := range a.config.Servers {
		if a.config.Servers[i].ID == id {
			return i
		}
	}
	return -1
}

func (a *app) ensureSelectedTarget() {
	a.mu.Lock()
	defer a.mu.Unlock()
	index := a.serverIndexLocked(a.config.SelectedID)
	if index < 0 {
		a.config.SelectedID = ""
		if len(a.config.Servers) > 0 {
			a.config.SelectedID = a.config.Servers[0].ID
		}
	}
	if target := a.selectedTargetLocked(); target != "" {
		_ = a.proxy.SetTarget(target)
	}
}

func defaultConfigPath() string {
	base, err := os.UserConfigDir()
	if err != nil {
		base, _ = os.Getwd()
	}
	return filepath.Join(base, appName, "config.json")
}

func loadConfig(path string) (config, error) {
	var cfg config
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func saveConfig(path string, cfg config) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func cloneConfig(cfg config) config {
	cloned := cfg
	cloned.Servers = append([]storedServer(nil), cfg.Servers...)
	sort.SliceStable(cloned.Servers, func(i, j int) bool {
		return cloned.Servers[i].LastPlayed.After(cloned.Servers[j].LastPlayed)
	})
	return cloned
}

func newID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func decodeJSON(r *http.Request, dst any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	return decoder.Decode(dst)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func openBrowser(url string) {
	var command *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		command = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		command = exec.Command("open", url)
	default:
		command = exec.Command("xdg-open", url)
	}
	_ = command.Start()
}

func runningInstance(url string) bool {
	client := &http.Client{Timeout: 600 * time.Millisecond}
	response, err := client.Get(url + "/api/state")
	if err != nil {
		return false
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return false
	}
	var state stateResponse
	if err := json.NewDecoder(response.Body).Decode(&state); err != nil {
		return false
	}
	return state.Protocol > 0 && state.Version != ""
}

func browserURL(address string) string {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "http://" + address
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}
