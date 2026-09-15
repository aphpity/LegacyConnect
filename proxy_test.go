package proxy

import (
	"bytes"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/sandertv/go-raknet"
)

func TestRelayRoundTrip(t *testing.T) {
	upstream, err := raknet.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()

	go func() {
		for {
			rawConn, err := upstream.Accept()
			if err != nil {
				return
			}
			conn := rawConn.(*raknet.Conn)
			go func() {
				defer conn.Close()
				for {
					packet, err := conn.ReadPacket()
					if err != nil {
						return
					}
					if _, err := conn.Write(append([]byte("echo:"), packet...)); err != nil {
						return
					}
				}
			}()
		}
	}()

	relay := New("127.0.0.1:0", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := relay.Start(upstream.Addr().String()); err != nil {
		t.Fatal(err)
	}
	defer relay.Stop()

	client, err := raknet.DialTimeout(relay.Status().Listen, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	want := []byte("legacyconnect-round-trip")
	if _, err := client.Write(want); err != nil {
		t.Fatal(err)
	}
	got, err := client.ReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, append([]byte("echo:"), want...)) {
		t.Fatalf("unexpected response: %q", got)
	}

	status := relay.Status()
	if status.Connections != 1 {
		t.Fatalf("connections = %d, want 1", status.Connections)
	}
	if status.BytesToServer == 0 || status.BytesToClient == 0 {
		t.Fatalf("traffic counters were not updated: %+v", status)
	}
}

func TestNormalizeAddress(t *testing.T) {
	got, err := NormalizeAddress("play.example.com")
	if err != nil {
		t.Fatal(err)
	}
	if got != "play.example.com:19132" {
		t.Fatalf("NormalizeAddress() = %q", got)
	}
}

func TestUnconnectedPong(t *testing.T) {
	request := make([]byte, 33)
	request[0] = 0x01
	binary.BigEndian.PutUint64(request[1:9], 12345)

	relay := New("127.0.0.1:0", slog.New(slog.NewTextHandler(io.Discard, nil)))
	response, ok := relay.unconnectedPong(request)
	if !ok {
		t.Fatal("unconnected ping was not recognised")
	}
	if response[0] != 0x1c {
		t.Fatalf("response ID = %#x, want 0x1c", response[0])
	}
	if got := binary.BigEndian.Uint64(response[1:9]); got != 12345 {
		t.Fatalf("ping time = %d, want 12345", got)
	}
	if !bytes.Equal(response[17:33], raknetMagic) {
		t.Fatal("response contains invalid RakNet magic")
	}
	if !strings.Contains(string(response[35:]), "MCPE;LegacyConnect;419;1.16.100.4;") {
		t.Fatalf("unexpected pong data: %q", response[35:])
	}
}

func TestServerAnswersUnconnectedPing(t *testing.T) {
	relay := New("127.0.0.1:0", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := relay.Start("127.0.0.1:19132"); err != nil {
		t.Fatal(err)
	}
	defer relay.Stop()
	if err := relay.SetProfile("1.12.1", 361); err != nil {
		t.Fatal(err)
	}

	conn, err := net.Dial("udp", relay.Status().Listen)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}

	request := make([]byte, 33)
	request[0] = 0x01
	binary.BigEndian.PutUint64(request[1:9], 67890)
	if _, err := conn.Write(request); err != nil {
		t.Fatal(err)
	}

	response := make([]byte, 512)
	n, err := conn.Read(response)
	if err != nil {
		t.Fatal(err)
	}
	response = response[:n]
	if len(response) < 35 || response[0] != 0x1c {
		t.Fatalf("unexpected unconnected pong: %x", response)
	}
	if got := binary.BigEndian.Uint64(response[1:9]); got != 67890 {
		t.Fatalf("ping time = %d, want 67890", got)
	}
	if !strings.Contains(string(response[35:]), "MCPE;LegacyConnect;361;1.12.1;") {
		t.Fatalf("unexpected profile in pong: %q", response[35:])
	}
}
