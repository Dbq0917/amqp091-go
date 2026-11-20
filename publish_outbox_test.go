package amqp091

import (
	"bytes"
	"testing"
	"time"
)

func reliableTestConfig() Config {
	config := defaultConfig()
	config.Recovery = &Recovery{
		ReconnectionConfig: &ReconnectionConfig{
			MaxRetryCount: 1,
			RetryInterval: time.Millisecond,
		},
	}
	return config
}

func waitForPendingPublishes(t *testing.T, ch *Channel, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := len(ch.publishOutbox.snapshot()); got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("expected %d pending publishes, got %d", want, len(ch.publishOutbox.snapshot()))
}

func TestReliablePublishUsesOneConfirmSelect(t *testing.T) {
	rwc, srv := newSession(t)
	t.Cleanup(func() { _ = rwc.Close() })

	const publishes = 5
	received := make(chan []byte, publishes)
	go func() {
		srv.connectionOpen()
		srv.channelOpen(1)
		srv.recv(1, &confirmSelect{})
		srv.send(1, &confirmSelectOk{})

		for i := 0; i < publishes; i++ {
			msg := &basicPublish{}
			srv.recv(1, msg)
			received <- append([]byte(nil), msg.Body...)
			srv.send(1, &basicAck{DeliveryTag: uint64(i + 1)})
		}
		srv.connectionClose()
	}()

	conn, err := Open(rwc, reliableTestConfig())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel failed: %v", err)
	}

	for i := 0; i < publishes; i++ {
		if err := ch.Publish("", "test", false, false, Publishing{Body: []byte{byte(i)}}); err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	for i := 0; i < publishes; i++ {
		select {
		case body := <-received:
			if !bytes.Equal(body, []byte{byte(i)}) {
				t.Fatalf("publish %d body mismatch: %v", i, body)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for publish %d", i)
		}
	}

	waitForPendingPublishes(t, ch, 0)
	if err := conn.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestQueuedPublishesReplayAfterRecovery(t *testing.T) {
	rwc, srv := newSession(t)
	t.Cleanup(func() { _ = rwc.Close() })

	const publishes = 3
	firstSession := make(chan struct{})
	replay := make(chan struct{})
	received := make(chan []byte, publishes)
	go func() {
		srv.connectionOpen()
		srv.channelOpen(1)
		srv.recv(1, &confirmSelect{})
		srv.send(1, &confirmSelectOk{})
		close(firstSession)

		<-replay
		srv.recv(1, &confirmSelect{})
		srv.send(1, &confirmSelectOk{})
		for i := 0; i < publishes; i++ {
			msg := &basicPublish{}
			srv.recv(1, msg)
			received <- append([]byte(nil), msg.Body...)
			srv.send(1, &basicAck{DeliveryTag: uint64(i + 1)})
		}
		srv.connectionClose()
	}()

	conn, err := Open(rwc, reliableTestConfig())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel failed: %v", err)
	}
	<-firstSession

	ch.publishOutbox.reset()
	ch.confirms.reset()
	ch.autoConfirmSession.Store(false)
	ch.setClosed()

	for i := 0; i < publishes; i++ {
		if err := ch.Publish("", "test", false, false, Publishing{Body: []byte{byte(i)}}); err != nil {
			t.Fatalf("queued Publish %d failed: %v", i, err)
		}
	}
	waitForPendingPublishes(t, ch, publishes)

	ch.closed.Store(false)
	close(replay)
	if err := ch.ensurePublishConfirms(); err != nil {
		t.Fatalf("ensurePublishConfirms failed: %v", err)
	}
	if err := ch.flushPendingPublishes(); err != nil {
		t.Fatalf("flushPendingPublishes failed: %v", err)
	}

	for i := 0; i < publishes; i++ {
		select {
		case body := <-received:
			if !bytes.Equal(body, []byte{byte(i)}) {
				t.Fatalf("replayed publish %d body mismatch: %v", i, body)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for replayed publish %d", i)
		}
	}

	waitForPendingPublishes(t, ch, 0)
	if err := conn.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestPublishOutboxAckMultiple(t *testing.T) {
	var outbox publishOutbox
	first := &pendingPublish{}
	second := &pendingPublish{}
	third := &pendingPublish{}
	outbox.add(first)
	outbox.add(second)
	outbox.add(third)
	outbox.bind(first, 1)
	outbox.bind(second, 2)
	outbox.bind(third, 3)

	outbox.ack(2, true)

	pending := outbox.snapshot()
	if len(pending) != 1 || pending[0] != third {
		t.Fatalf("expected only the third publishing to remain, got %d entries", len(pending))
	}
}

func TestNackedPublishRemainsPendingForReplay(t *testing.T) {
	ch := &Channel{confirms: newConfirms()}
	ch.autoConfirmSession.Store(true)
	pending := &pendingPublish{}
	ch.publishOutbox.add(pending)
	ch.publishOutbox.bind(pending, 1)

	ch.publishConfirmation(1, false, false)

	if got := len(ch.publishOutbox.snapshot()); got != 1 {
		t.Fatalf("expected the nacked publishing to remain pending, got %d", got)
	}
}

func TestClonePublishingForRetryCopiesBodyAndHeaders(t *testing.T) {
	body := []byte("body")
	header := []byte("header")
	msg := Publishing{
		Body:    body,
		Headers: Table{"key": header},
	}

	clone := clonePublishingForRetry(msg)
	body[0] = 'B'
	header[0] = 'H'

	if string(clone.Body) != "body" {
		t.Fatalf("body was not copied: %q", clone.Body)
	}
	if got := string(clone.Headers["key"].([]byte)); got != "header" {
		t.Fatalf("header was not copied: %q", got)
	}
}
