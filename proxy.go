package proxy

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sandertv/go-raknet"
)

const (
	DefaultProxyAddress = "127.0.0.1:19132"
	ProtocolVersion     = 419
	GameVersion         = "1.16.100.4"

	maxDatagramSize = 65535
	sessionIdleTime = 2 * time.Minute
)

var raknetMagic = []byte{
	0x00, 0xff, 0xff, 0x00, 0xfe, 0xfe, 0xfe, 0xfe,
	0xfd, 0xfd, 0xfd, 0xfd, 0x12, 0x34, 0x56, 0x78,
}

// Status describes the current proxy state.
type Status struct {
	Running           bool   `json:"running"`
	Listen            string `json:"listen"`
	Target            string `json:"target"`
	Version           string `json:"version"`
	Protocol          int    `json:"protocol"`
	Connections       int64  `json:"connections"`
	ActiveConnections int64  `json:"activeConnections"`
	BytesToServer     int64  `json:"bytesToServer"`
	BytesToClient     int64  `json:"bytesToClient"`
	LastError         string `json:"lastError,omitempty"`
}

// Pong is the useful subset of a Bedrock unconnected pong.
type Pong struct {
	Address    string `json:"address"`
	Motd       string `json:"motd"`
	Protocol   int    `json:"protocol"`
	Version    string `json:"version"`
	Players    int    `json:"players"`
	MaxPlayers int    `json:"maxPlayers"`
	LevelName  string `json:"levelName"`
	GameMode   string `json:"gameMode"`
	LatencyMS  int64  `json:"latencyMs"`
	Compatible bool   `json:"compatible"`
}

type udpSession struct {
	key       string
	client    net.Addr
	upstream  *net.UDPConn
	lastSeen  atomic.Int64
	closeOnce sync.Once
}

func (s *udpSession) close() {
	s.closeOnce.Do(func() {
		_ = s.upstream.Close()
	})
}

// Server is a transparent RakNet relay. It forwards UDP datagrams without
// terminating the RakNet session, so Minecraft sees one end-to-end connection.
type Server struct {
	mu         sync.RWMutex
	listenAddr string
	target     string
	backend    string
	version    string
	protocol   int
	listener   net.PacketConn
	running    bool
	lastError  string
	sessions   map[string]*udpSession
	done       chan struct{}
	log        *slog.Logger
	serverGUID uint64

	connections      atomic.Int64
	activeConnection atomic.Int64
	bytesToServer    atomic.Int64
	bytesToClient    atomic.Int64
}

func New(listenAddr string, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	normalized, err := NormalizeListenAddress(listenAddr)
	if err != nil {
		normalized = DefaultProxyAddress
	}
	return &Server{
		listenAddr: normalized,
		sessions:   make(map[string]*udpSession),
		log:        log,
		serverGUID: uint64(time.Now().UnixNano()),
		version:    GameVersion,
		protocol:   ProtocolVersion,
	}
}

// Start begins accepting Bedrock 1.16.100 clients and relays their raw RakNet
// datagrams to target.
func (s *Server) Start(target string) error {
	normalizedTarget, err := NormalizeAddress(target)
	if err != nil {
		return err
	}
	backend := resolveBestTarget(normalizedTarget, 1500*time.Millisecond)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return errors.New("proxy is already running")
	}

	listener, err := net.ListenPacket("udp", s.listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.listenAddr, err)
	}

	s.listener = listener
	s.target = normalizedTarget
	s.backend = backend
	s.running = true
	s.lastError = ""
	s.done = make(chan struct{})
	go s.acceptLoop(listener, s.done)
	go s.cleanupLoop(s.done)
	return nil
}

// SetTarget changes the destination used by new client sessions.
func (s *Server) SetTarget(target string) error {
	normalized, err := NormalizeAddress(target)
	if err != nil {
		return err
	}
	backend := resolveBestTarget(normalized, 1500*time.Millisecond)
	s.mu.Lock()
	s.target = normalized
	s.backend = backend
	s.mu.Unlock()
	return nil
}

// SetProfile changes the version and protocol advertised to Minecraft in the
// local server list. Forwarded traffic is not translated.
func (s *Server) SetProfile(version string, protocol int) error {
	version = strings.TrimSpace(version)
	if version == "" {
		return errors.New("version is empty")
	}
	if protocol < 1 || protocol > 1<<20 {
		return errors.New("protocol must be between 1 and 1048576")
	}
	s.mu.Lock()
	s.version = version
	s.protocol = protocol
	s.mu.Unlock()
	return nil
}

// Stop closes the listener and every active relay session.
func (s *Server) Stop() error {
	s.mu.Lock()
	listener := s.listener
	done := s.done
	s.listener = nil
	s.done = nil
	s.running = false
	sessions := make([]*udpSession, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	s.sessions = make(map[string]*udpSession)
	s.mu.Unlock()

	if done != nil {
		close(done)
	}
	for _, session := range sessions {
		session.close()
	}
	s.activeConnection.Store(0)
	if listener == nil {
		return nil
	}
	return listener.Close()
}

func (s *Server) Status() Status {
	s.mu.RLock()
	status := Status{
		Running:   s.running,
		Listen:    s.listenAddr,
		Target:    s.target,
		Version:   s.version,
		Protocol:  s.protocol,
		LastError: s.lastError,
	}
	if s.listener != nil {
		status.Listen = s.listener.LocalAddr().String()
	}
	s.mu.RUnlock()

	status.Connections = s.connections.Load()
	status.ActiveConnections = s.activeConnection.Load()
	status.BytesToServer = s.bytesToServer.Load()
	status.BytesToClient = s.bytesToClient.Load()
	return status
}

func (s *Server) acceptLoop(listener net.PacketConn, done <-chan struct{}) {
	buffer := make([]byte, maxDatagramSize)
	for {
		n, clientAddr, err := listener.ReadFrom(buffer)
		if err != nil {
			select {
			case <-done:
				return
			default:
			}
			s.mu.Lock()
			if s.listener == listener {
				s.running = false
				s.lastError = err.Error()
			}
			s.mu.Unlock()
			if !isClosedError(err) {
				s.log.Warn("proxy listener stopped", "error", err)
			}
			return
		}

		packet := append([]byte(nil), buffer[:n]...)
		if response, ok := s.unconnectedPong(packet); ok {
			if _, err := listener.WriteTo(response, clientAddr); err != nil {
				s.setLastError(err)
			}
			continue
		}
		s.forward(listener, clientAddr, packet)
	}
}

func (s *Server) forward(listener net.PacketConn, clientAddr net.Addr, packet []byte) {
	session, err := s.sessionFor(listener, clientAddr)
	if err != nil {
		s.setLastError(err)
		s.log.Warn("failed to create relay session", "client", clientAddr.String(), "error", err)
		return
	}
	session.lastSeen.Store(time.Now().UnixNano())

	n, err := session.upstream.Write(packet)
	if n > 0 {
		s.bytesToServer.Add(int64(n))
	}
	if err != nil {
		s.setLastError(err)
		s.removeSession(session)
	}
}

func (s *Server) sessionFor(listener net.PacketConn, clientAddr net.Addr) (*udpSession, error) {
	key := clientAddr.Network() + "|" + clientAddr.String()

	s.mu.RLock()
	if session := s.sessions[key]; session != nil {
		s.mu.RUnlock()
		return session, nil
	}
	target := s.backend
	running := s.running
	s.mu.RUnlock()
	if !running {
		return nil, errors.New("proxy is not running")
	}

	upstreamAddr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", target, err)
	}
	upstream, err := net.DialUDP("udp", nil, upstreamAddr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", target, err)
	}

	session := &udpSession{
		key:      key,
		client:   clientAddr,
		upstream: upstream,
	}
	session.lastSeen.Store(time.Now().UnixNano())

	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		session.close()
		return nil, errors.New("proxy is not running")
	}
	if s.sessions == nil {
		s.sessions = make(map[string]*udpSession)
	}
	if existing := s.sessions[key]; existing != nil {
		s.mu.Unlock()
		session.close()
		return existing, nil
	}
	s.sessions[key] = session
	s.mu.Unlock()

	s.connections.Add(1)
	s.activeConnection.Add(1)
	go s.readUpstream(listener, session)
	return session, nil
}

func (s *Server) readUpstream(listener net.PacketConn, session *udpSession) {
	defer s.removeSession(session)

	buffer := make([]byte, maxDatagramSize)
	for {
		n, err := session.upstream.Read(buffer)
		if err != nil {
			if !isClosedError(err) {
				s.setLastError(err)
			}
			return
		}
		session.lastSeen.Store(time.Now().UnixNano())
		written, err := listener.WriteTo(buffer[:n], session.client)
		if written > 0 {
			s.bytesToClient.Add(int64(written))
		}
		if err != nil {
			s.setLastError(err)
			return
		}
	}
}

func (s *Server) removeSession(session *udpSession) {
	removed := false
	s.mu.Lock()
	if current := s.sessions[session.key]; current == session {
		delete(s.sessions, session.key)
		removed = true
	}
	s.mu.Unlock()
	if removed {
		s.activeConnection.Add(-1)
	}
	session.close()
}

func (s *Server) cleanupLoop(done <-chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-sessionIdleTime).UnixNano()
			s.mu.RLock()
			var expired []*udpSession
			for _, session := range s.sessions {
				if session.lastSeen.Load() < cutoff {
					expired = append(expired, session)
				}
			}
			s.mu.RUnlock()
			for _, session := range expired {
				s.removeSession(session)
			}
		}
	}
}

func resolveBestTarget(address string, timeout time.Duration) string {
	host, port, err := net.SplitHostPort(address)
	if err != nil || net.ParseIP(host) != nil {
		return address
	}
	ips, err := net.LookupIP(host)
	if err != nil || len(ips) < 2 {
		return address
	}

	type result struct {
		address string
		latency time.Duration
	}
	results := make(chan result, len(ips))
	for _, ip := range ips {
		ip := ip
		go func() {
			candidate := net.JoinHostPort(ip.String(), port)
			start := time.Now()
			if _, err := raknet.PingTimeout(candidate, timeout); err != nil {
				results <- result{latency: -1}
				return
			}
			results <- result{address: candidate, latency: time.Since(start)}
		}()
	}

	best := result{latency: -1}
	for range ips {
		candidate := <-results
		if candidate.latency >= 0 && (best.latency < 0 || candidate.latency < best.latency) {
			best = candidate
		}
	}
	if best.address == "" {
		return address
	}
	return best.address
}

func (s *Server) setLastError(err error) {
	if err == nil || isClosedError(err) {
		return
	}
	s.mu.Lock()
	s.lastError = err.Error()
	s.mu.Unlock()
}

func (s *Server) unconnectedPong(packet []byte) ([]byte, bool) {
	if len(packet) < 33 || packet[0] != 0x01 {
		return nil, false
	}
	pingTime := binary.BigEndian.Uint64(packet[1:9])
	s.mu.RLock()
	protocol := s.protocol
	version := s.version
	serverGUID := s.serverGUID
	s.mu.RUnlock()
	data := []byte(fmt.Sprintf(
		"MCPE;LegacyConnect;%d;%s;0;2;%d;LegacyConnect;Survival;",
		protocol,
		version,
		serverGUID,
	))

	response := make([]byte, 35+len(data))
	response[0] = 0x1c
	binary.BigEndian.PutUint64(response[1:9], pingTime)
	binary.BigEndian.PutUint64(response[9:17], serverGUID)
	copy(response[17:33], raknetMagic)
	binary.BigEndian.PutUint16(response[33:35], uint16(len(data)))
	copy(response[35:], data)
	return response, true
}

// Ping queries a Bedrock server using RakNet 10.
func Ping(address string, timeout time.Duration, expectedProtocol int) (Pong, error) {
	normalized, err := NormalizeAddress(address)
	if err != nil {
		return Pong{}, err
	}

	start := time.Now()
	data, err := raknet.PingTimeout(normalized, timeout)
	if err != nil {
		return Pong{}, err
	}
	latency := time.Since(start).Milliseconds()

	parts := strings.Split(string(data), ";")
	if len(parts) < 6 || parts[0] != "MCPE" {
		return Pong{}, errors.New("server returned invalid Bedrock pong data")
	}
	protocol, _ := strconv.Atoi(parts[2])
	players, _ := strconv.Atoi(parts[4])
	maxPlayers, _ := strconv.Atoi(parts[5])

	pong := Pong{
		Address:    normalized,
		Motd:       parts[1],
		Protocol:   protocol,
		Version:    parts[3],
		Players:    players,
		MaxPlayers: maxPlayers,
		LatencyMS:  latency,
		Compatible: protocol == expectedProtocol,
	}
	if len(parts) > 7 {
		pong.LevelName = parts[7]
	}
	if len(parts) > 8 {
		pong.GameMode = parts[8]
	}
	return pong, nil
}

// NormalizeAddress adds the default Bedrock port when no port is provided.
func NormalizeAddress(address string) (string, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return "", errors.New("server address is empty")
	}
	if !strings.Contains(address, ":") {
		address = net.JoinHostPort(address, "19132")
	}
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return "", fmt.Errorf("invalid server address: %w", err)
	}
	if strings.TrimSpace(host) == "" {
		return "", errors.New("server host is empty")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", errors.New("server port must be between 1 and 65535")
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}

func NormalizeListenAddress(address string) (string, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		address = DefaultProxyAddress
	}
	if !strings.Contains(address, ":") {
		address = net.JoinHostPort(address, "19132")
	}
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return "", fmt.Errorf("invalid listen address: %w", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 0 || port > 65535 {
		return "", errors.New("listen port must be between 0 and 65535")
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}

func isClosedError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "closed") ||
		strings.Contains(text, "shutdown") ||
		strings.Contains(text, "use of closed") ||
		strings.Contains(text, "forcibly closed")
}
