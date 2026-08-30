package client

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestExecuteCommandFramesRequestAndConsumesEndMarker(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	serverErr := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverErr <- acceptErr
			return
		}
		defer conn.Close()

		line, readErr := bufio.NewReader(conn).ReadString('\n')
		if readErr != nil {
			serverErr <- readErr
			return
		}
		if line != "KV SET alpha beta\n" {
			serverErr <- fmt.Errorf("unexpected command frame %q", line)
			return
		}
		_, writeErr := conn.Write([]byte("stored\n" + EndMarker + "\n"))
		serverErr <- writeErr
	}()

	host, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}

	client := NewClient()
	if !client.Connect(host, port) {
		t.Fatal("client did not connect to loopback test server")
	}
	t.Cleanup(client.Disconnect)

	response, err := client.ExecuteCommand("KV SET alpha beta")
	if err != nil {
		t.Fatal(err)
	}
	if response != "stored" {
		t.Fatalf("unexpected response %q", response)
	}

	select {
	case err := <-serverErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not finish command exchange")
	}
}

func TestDisconnectedClientFailsClosed(t *testing.T) {
	client := NewClient()

	if client.IsConnected() {
		t.Fatal("new client reported a connection")
	}
	if _, err := client.ExecuteCommand("KV PING"); err == nil {
		t.Fatal("command without a connection unexpectedly succeeded")
	}
}
