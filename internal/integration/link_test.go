package integration_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/printer"
)

// assertOfflineSend protects immediate rejection without a connection wait or replay queue.
func assertOfflineSend(t *testing.T, links *printer.Links, serial string) {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- links.Send(serial, []byte("OFFLINE-DO-NOT-REPLAY")) }()
	select {
	case err := <-result:
		if !errors.Is(err, printer.ErrOffline) {
			t.Fatalf("offline Send: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("offline Send waited for a connection")
	}
}

// assertConnectTraffic checks the real MQTT packets for one connection's subscribe and warmup.
func assertConnectTraffic(t *testing.T, p *fakePrinter, serial string, subscriptions int) {
	t.Helper()
	waitFor(t, 5*time.Second, func() bool { return p.rec.count(requestTopic(serial), warmupPayload()) == 1 })
	p.rec.mu.Lock()
	defer p.rec.mu.Unlock()
	if p.rec.connects != 1 || p.rec.subs[reportTopic(serial)] != subscriptions {
		t.Fatalf("connects=%d subscriptions=%d, want 1 and %d", p.rec.connects, p.rec.subs[reportTopic(serial)], subscriptions)
	}
	for _, sub := range p.rec.subEvents {
		if sub.filter != reportTopic(serial) || sub.qos != 0 {
			t.Fatalf("SUBSCRIBE = %+v", sub)
		}
	}
	warmup := p.rec.seen[requestTopic(serial)+"|"+warmupPayload()]
	if warmup.count != 1 || warmup.qos != 0 || warmup.retain {
		t.Fatalf("pushall = %+v, want one QoS 0 non-retained publish", warmup)
	}
	if p.rec.seen[requestTopic(serial)+"|OFFLINE-DO-NOT-REPLAY"].count != 0 {
		t.Fatal("offline command was replayed")
	}
}

// TestBootOfflineWildcardAndReconnect protects boot retry, wildcard availability, byte-exact reports, and reconnect warmups.
func TestBootOfflineWildcardAndReconnect(t *testing.T) {
	live := newFakePrinter(t, freePort(t))
	offlinePort := freePort(t)
	p := startProxy(t, []config.Printer{printerSpec(live.port, serial1), printerSpec(offlinePort, serial2)})
	assertOfflineSend(t, p.links, serial2)
	if statuses := p.links.Status(); len(statuses) != 2 || statuses[serial2] {
		t.Fatalf("configured status: %v", statuses)
	}
	client := connect(t, p, "boot-wildcard", "bblp", accessCode, false)
	client.publish(t, requestTopic(serial2), "OFFLINE-DO-NOT-REPLAY", 0, false)
	// The SUBACK below is the ordering barrier: mochi processes this client's
	// PUBLISH before the following SUBSCRIBE on the same connection.
	if got := client.subscribe(t, "device/+/report", 0); got != 0 {
		t.Fatalf("wildcard SUBACK with offline printer = %#x", got)
	}
	waitFor(t, 5*time.Second, func() bool { return p.links.Online(serial1) })
	payload := " \x00\xff{\"print\":{\"gcode_state\":\"RUNNING\"}}\n"
	live.publish(t, reportTopic(serial1), payload)
	waitFor(t, 5*time.Second, func() bool { return client.box.has(reportTopic(serial1), payload) })
	assertConnectTraffic(t, live, serial1, 1)

	// The pre-existing wildcard requires no new SUBSCRIBE after a boot-offline printer starts.
	late := newFakePrinter(t, offlinePort)
	waitFor(t, 15*time.Second, func() bool { return p.links.Online(serial2) })
	late.publish(t, reportTopic(serial2), payload)
	waitFor(t, 5*time.Second, func() bool { return client.box.has(reportTopic(serial2), payload) })
	assertConnectTraffic(t, late, serial2, 1)
	late.stop()
	waitFor(t, 5*time.Second, func() bool { return !p.links.Online(serial2) })
	assertOfflineSend(t, p.links, serial2)
	reconnected := newFakePrinter(t, offlinePort)
	waitFor(t, 15*time.Second, func() bool { return p.links.Online(serial2) })
	reconnected.publish(t, reportTopic(serial2), "after-reconnect\x00\xff")
	waitFor(t, 5*time.Second, func() bool { return client.box.has(reportTopic(serial2), "after-reconnect\x00\xff") })
	assertConnectTraffic(t, reconnected, serial2, 1)
	p.logs.mu.Lock()
	defer p.logs.mu.Unlock()
	if !slices.Equal(p.logs.online[serial1], []bool{true}) || !slices.Equal(p.logs.online[serial2], []bool{true, false, true}) {
		t.Fatalf("online callbacks were not transitions: %v", p.logs.online)
	}
}

// TestRefusedSubscriptionRetriesOnOpenConnection protects SUBACK refusal, offline commands, and retry without reconnect.
func TestRefusedSubscriptionRetriesOnOpenConnection(t *testing.T) {
	fp := newFakePrinter(t, freePort(t), true)
	p := startProxy(t, []config.Printer{printerSpec(fp.port, serial1)})
	// The recorder sees the refused SUBSCRIBE attempt on the open connection.
	waitFor(t, 5*time.Second, func() bool {
		fp.rec.mu.Lock()
		defer fp.rec.mu.Unlock()
		return fp.rec.subs[reportTopic(serial1)] >= 1
	})
	if p.links.Online(serial1) || fp.rec.count(requestTopic(serial1), warmupPayload()) != 0 {
		t.Fatal("refused subscription became online or sent pushall")
	}
	assertOfflineSend(t, p.links, serial1)
	client := connect(t, p, "refused", "bblp", accessCode, false)
	if got := client.subscribe(t, reportTopic(serial1), 0); got != 0 {
		t.Fatalf("downstream SUBACK during refusal: %#x", got)
	}
	fp.rec.mu.Lock()
	fp.rec.refuse = false
	fp.rec.mu.Unlock()
	waitFor(t, 15*time.Second, func() bool { return p.links.Online(serial1) })
	fp.publish(t, reportTopic(serial1), "after-subscribe-retry")
	waitFor(t, 5*time.Second, func() bool { return client.box.has(reportTopic(serial1), "after-subscribe-retry") })
	assertConnectTraffic(t, fp, serial1, 2)
	p.logs.mu.Lock()
	defer p.logs.mu.Unlock()
	if !slices.Equal(p.logs.online[serial1], []bool{true}) {
		t.Fatalf("refusal callbacks: %v", p.logs.online)
	}
}

// TestCommandsUseQoS0WirePayloads protects the command helpers' public MQTT contract.
func TestCommandsUseQoS0WirePayloads(t *testing.T) {
	fp := newFakePrinter(t, freePort(t))
	p := startProxy(t, []config.Printer{printerSpec(fp.port, serial1)})
	waitFor(t, 5*time.Second, func() bool { return p.links.Online(serial1) })
	for _, tc := range []struct {
		send    func() error
		payload string
	}{
		{func() error { return p.links.Pause(serial1) }, `{"print":{"sequence_id":"0","command":"pause"}}`},
		{func() error { return p.links.Resume(serial1) }, `{"print":{"sequence_id":"0","command":"resume"}}`},
		{func() error { return p.links.Stop(serial1) }, `{"print":{"sequence_id":"0","command":"stop"}}`},
		{func() error { return p.links.Light(serial1, true) }, `{"system":{"sequence_id":"0","command":"ledctrl","led_node":"chamber_light","led_mode":"on","led_on_time":500,"led_off_time":500,"loop_times":0,"interval_time":0}}`},
		{func() error { return p.links.Light(serial1, false) }, `{"system":{"sequence_id":"0","command":"ledctrl","led_node":"chamber_light","led_mode":"off","led_on_time":500,"led_off_time":500,"loop_times":0,"interval_time":0}}`},
		{func() error { return p.links.Speed(serial1, 4) }, `{"print":{"sequence_id":"0","command":"print_speed","param":"4"}}`},
	} {
		if err := tc.send(); err != nil {
			t.Fatal(err)
		}
		waitFor(t, 5*time.Second, func() bool { return fp.rec.count(requestTopic(serial1), tc.payload) == 1 })
		fp.rec.mu.Lock()
		item := fp.rec.seen[requestTopic(serial1)+"|"+tc.payload]
		fp.rec.mu.Unlock()
		if item.qos != 0 || item.retain {
			t.Fatalf("command flags: %+v", item)
		}
	}
	if err := p.links.Speed(serial1, 5); err == nil {
		t.Fatal("invalid speed accepted")
	}
}
