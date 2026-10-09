package integration_test

import (
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

const alias1 = "ALIASPRINTER1"

// aliasedPrinterSpec builds a config entry with a downstream alias.
func aliasedPrinterSpec(port int, serial, alias string) config.Printer {
	spec := printerSpec(port, serial)
	spec.Alias = alias
	return spec
}

// TestAliasRoutingEndToEnd pins the alias contract: the aliased printer is
// addressed downstream by its alias while the real serial serves upstream,
// and an unaliased printer keeps its serial on both hops.
func TestAliasRoutingEndToEnd(t *testing.T) {
	p1 := newFakePrinter(t, freePort(t))
	p2 := newFakePrinter(t, freePort(t))
	p := startProxy(t, []config.Printer{
		aliasedPrinterSpec(p1.port, serial1, alias1),
		printerSpec(p2.port, serial2),
	})

	aliasedReport := reportTopic(alias1)
	aliasedRequest := requestTopic(alias1)

	c := connect(t, p, "alias-c", "bblp", accessCode, true)
	if got := c.subscribe(t, aliasedReport, 1); got != 0 {
		t.Fatalf("alias subscribe granted %#x, want 0", got)
	}
	waitFor(t, 5*time.Second, func() bool {
		return p1.rec.subCount(reportTopic(serial1)) >= 1
	})

	// (a) A printer report on the real serial reaches the alias subscriber
	// on the alias topic with a byte-identical payload.
	reportPayload := `{"print":{"layer":7},"raw":"` + "bytes\x00\xff" + `"}`
	p1.publish(t, reportTopic(serial1), reportPayload)
	waitFor(t, 5*time.Second, func() bool {
		return c.box.has(aliasedReport, reportPayload)
	})
	if c.box.has(reportTopic(serial1), reportPayload) {
		t.Fatal("aliased report also arrived on the real serial topic")
	}

	// (b) A client publish to the alias request topic reaches the fake
	// printer on the real serial topic with a byte-identical payload.
	cmdPayload := `{"print":{"sequence_id":"9","command":"pause"}}`
	c.publish(t, aliasedRequest, cmdPayload, 0, false)
	waitFor(t, 5*time.Second, func() bool {
		return p1.rec.count(reportTopic(serial1), cmdPayload) == 0 &&
			p1.rec.count(requestTopic(serial1), cmdPayload) == 1
	})
	time.Sleep(300 * time.Millisecond)
	if got := p1.rec.count(requestTopic(serial1), cmdPayload); got != 1 {
		t.Fatalf("printer received %d copies of the alias request, want exactly 1", got)
	}

	// (c) The aliased printer's real serial is refused downstream.
	if got := c.subscribe(t, reportTopic(serial1), 1); got != 0x80 {
		t.Fatalf("real serial subscribe granted %#x, want 0x80", got)
	}
	denied := connect(t, p, "alias-denied", "bblp", accessCode, true)
	deniedPayload := `{"print":{"sequence_id":"10","command":"resume"}}`
	tok := denied.cl.Publish(requestTopic(serial1), 1, false, []byte(deniedPayload))
	_ = tok.WaitTimeout(5 * time.Second)
	select {
	case <-denied.lost:
	case <-time.After(5 * time.Second):
		t.Fatal("client publishing to the real serial was not disconnected")
	}
	time.Sleep(300 * time.Millisecond)
	if n := p1.rec.count(requestTopic(serial1), deniedPayload); n != 0 {
		t.Fatalf("real-serial publish reached the printer %d times, want 0", n)
	}

	// (d) A wildcard subscriber receives the aliased report exactly once on
	// the alias topic and the unaliased report on its serial topic.
	wild := connect(t, p, "alias-wild", "bblp", accessCode, true)
	if got := wild.subscribe(t, "device/+/report", 1); got >= 0x80 {
		t.Fatalf("wildcard subscribe granted %#x, want 0", got)
	}
	waitFor(t, 5*time.Second, func() bool {
		return p2.rec.subCount(reportTopic(serial2)) >= 1
	})
	wildReport := "wild-v1"
	plainReport := "plain-v1"
	p1.publish(t, reportTopic(serial1), wildReport)
	p2.publish(t, reportTopic(serial2), plainReport)
	waitFor(t, 5*time.Second, func() bool {
		return wild.box.has(aliasedReport, wildReport) && wild.box.has(reportTopic(serial2), plainReport)
	})
	time.Sleep(500 * time.Millisecond)
	wild.box.mu.Lock()
	aliasCount := wild.box.seen[aliasedReport+"|"+wildReport]
	serialLeak := wild.box.seen[reportTopic(serial1)+"|"+wildReport]
	plainCount := wild.box.seen[reportTopic(serial2)+"|"+plainReport]
	wild.box.mu.Unlock()
	if aliasCount != 1 {
		t.Fatalf("wildcard received the aliased report %d times, want exactly once", aliasCount)
	}
	if serialLeak != 0 {
		t.Fatalf("wildcard received the aliased report on the real serial %d times, want 0", serialLeak)
	}
	if plainCount != 1 {
		t.Fatalf("wildcard received the plain report %d times, want exactly once", plainCount)
	}
}
