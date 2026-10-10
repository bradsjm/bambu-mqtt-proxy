// Package printer owns one always-on MQTT link per configured printer.
package printer

import (
	"context"
	"crypto/tls"
	"log/slog"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"bambu-mqtt-proxy/internal/config"
)

// link owns a printer's client and its subscription lifecycle.
type link struct {
	// serial identifies the configured printer.
	serial string
	// client reconnects and delivers reports in wire order.
	client mqtt.Client
	// log records subscription and connectivity failures.
	log *slog.Logger
	// onReport forwards a report without blocking or publishing upstream.
	onReport func(serial, topic string, payload []byte)
	// onOnline observes changes to connected-and-subscribed status.
	onOnline func(serial string, online bool)
	// mu protects subscription cancellation and online status.
	mu sync.Mutex
	// transitions serializes status changes and their notifications.
	transitions sync.Mutex
	// online is true after the report subscription succeeds.
	online bool
	// closed prevents callbacks from reviving a stopped link.
	closed bool
	// cancel ends subscription retries on connection loss or shutdown.
	cancel context.CancelFunc
}

// newLink builds a client with the printer's credentials and TLS settings.
func newLink(spec config.Printer, log *slog.Logger, onReport func(string, string, []byte), onOnline func(string, bool)) *link {
	l := &link{serial: spec.Serial, log: log.With("serial", spec.Serial), onReport: onReport, onOnline: onOnline}
	scheme := "tcp"
	if spec.TLS {
		scheme = "tls"
	}
	opts := mqtt.NewClientOptions().
		AddBroker(scheme + "://" + spec.Address).
		SetClientID("bmbpx-" + spec.Serial).
		SetUsername(spec.Username).
		SetPassword(spec.Password).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(10 * time.Second).
		SetMaxReconnectInterval(30 * time.Second).
		SetKeepAlive(30 * time.Second).
		SetPingTimeout(10 * time.Second).
		SetConnectTimeout(10 * time.Second).
		SetWriteTimeout(5 * time.Second).
		SetOrderMatters(true).
		SetStore(mqtt.NewMemoryStore()).
		SetOnConnectHandler(l.connected).
		SetReconnectingHandler(l.reconnecting).
		SetConnectionLostHandler(l.lost)
	if spec.TLS {
		opts.SetTLSConfig(&tls.Config{InsecureSkipVerify: spec.InsecureSkipVerify})
	}
	l.client = mqtt.NewClient(opts)
	return l
}

// connected establishes the report subscription and retries refusals on the open connection.
func (l *link) connected(client mqtt.Client) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	if l.cancel != nil {
		l.cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	l.mu.Unlock()
	defer cancel()
	l.setOnline(nil, false)

	topic := "device/" + l.serial + "/report"
	for ctx.Err() == nil && client.IsConnectionOpen() {
		token := client.Subscribe(topic, 0, l.report)
		select {
		case <-ctx.Done():
			return
		case <-token.Done():
		}
		if ctx.Err() != nil || !client.IsConnectionOpen() {
			return
		}
		granted, ok := token.(*mqtt.SubscribeToken).Result()[topic]
		if token.Error() == nil && ok && granted == 0 {
			// The connect warmup is attempted exactly once, never retried.
			push := client.Publish("device/"+l.serial+"/request", 0, false, []byte(`{"pushing":{"sequence_id":"0","command":"pushall"}}`))
			if !push.WaitTimeout(5 * time.Second) {
				l.log.Warn("printer warmup timed out")
			} else if err := push.Error(); err != nil {
				l.log.Warn("printer warmup failed", "error", err)
			}
			l.setOnline(ctx, true)
			return
		}
		l.log.Warn("printer report subscription refused", "granted", granted, "error", token.Error(), "retry_delay_seconds", 10)
		timer := time.NewTimer(10 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// report delivers the printer's bytes directly to the non-blocking observer.
func (l *link) report(_ mqtt.Client, message mqtt.Message) {
	if l.onReport != nil {
		l.onReport(l.serial, message.Topic(), message.Payload())
	}
}

// reconnecting ends the old subscription loop before Paho opens a replacement socket.
func (l *link) reconnecting(_ mqtt.Client, _ *mqtt.ClientOptions) {
	l.mu.Lock()
	if l.cancel != nil {
		l.cancel()
	}
	l.mu.Unlock()
	l.setOnline(nil, false)
}

// lost cancels subscription retries and marks the printer offline.
func (l *link) lost(client mqtt.Client, err error) {
	l.transitions.Lock()
	defer l.transitions.Unlock()
	l.mu.Lock()
	// Paho dispatches loss and connect callbacks on separate goroutines.
	// A delayed loss must not take a newly opened connection offline.
	if client.IsConnectionOpen() || l.closed {
		l.mu.Unlock()
		return
	}
	if l.cancel != nil {
		l.cancel()
	}
	changed := l.online
	l.online = false
	l.mu.Unlock()
	if changed && l.onOnline != nil {
		l.onOnline(l.serial, false)
	}
	l.log.Warn("printer connection lost", "error", err)
}

// setOnline computes each transition under the lock and notifies it in order.
func (l *link) setOnline(ctx context.Context, online bool) {
	l.transitions.Lock()
	defer l.transitions.Unlock()
	l.mu.Lock()
	if online {
		online = !l.closed && ctx.Err() == nil && l.client.IsConnectionOpen()
	}
	changed := l.online != online
	l.online = online
	l.mu.Unlock()
	if changed && l.onOnline != nil {
		l.onOnline(l.serial, online)
	}
}

// isOnline reports whether this link is connected and subscribed.
func (l *link) isOnline() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.closed && l.online && l.client.IsConnectionOpen()
}

// close ends retries and disconnects the client's socket.
func (l *link) close() {
	l.mu.Lock()
	l.closed = true
	if l.cancel != nil {
		l.cancel()
	}
	l.mu.Unlock()
	l.client.Disconnect(100)
	l.setOnline(nil, false)
}
