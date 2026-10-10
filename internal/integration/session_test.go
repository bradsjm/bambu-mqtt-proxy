package integration_test

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"bambu-mqtt-proxy/internal/config"
)

// TestWillNeverFansOutOrRetains protects downstream will suppression in both auth modes.
func TestWillNeverFansOutOrRetains(t *testing.T) {
	for _, mode := range []string{config.AuthModePrinter, config.AuthModeAcceptAll} {
		t.Run(mode, func(t *testing.T) {
			p1 := newFakePrinter(t, freePort(t))
			p := startProxyAuth(t, mode, []config.Printer{printerSpec(p1.port, serial1)})
			waitFor(t, 5*time.Second, func() bool { return p.links.Online(serial1) })
			sub := connect(t, p, "will-sub", "bblp", accessCode, false)
			for _, topic := range []string{reportTopic(serial1), requestTopic(serial1)} {
				if got := sub.subscribe(t, topic, 0); got != 0 {
					t.Fatalf("subscribe %s: %#x", topic, got)
				}
			}
			for i, topic := range []string{reportTopic(serial1), requestTopic(serial1)} {
				address := net.JoinHostPort("127.0.0.1", strconv.Itoa(p.port))
				var socket net.Conn
				opts := mqtt.NewClientOptions().
					AddBroker("tls://"+address).
					SetClientID(fmt.Sprintf("will-%d", i)).SetUsername("bblp").SetPassword(accessCode).
					SetAutoReconnect(false).SetConnectTimeout(5*time.Second).
					SetTLSConfig(&tls.Config{InsecureSkipVerify: true}).SetWill(topic, "SUPPRESSED-WILL", 0, true)
				opts.SetCustomOpenConnectionFn(func(_ *url.URL, _ mqtt.ClientOptions) (net.Conn, error) {
					var err error
					socket, err = tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", address, &tls.Config{InsecureSkipVerify: true})
					return socket, err
				})
				client := mqtt.NewClient(opts)
				t.Cleanup(func() { client.Disconnect(0) })
				token := client.Connect()
				if !token.WaitTimeout(5*time.Second) || token.Error() != nil {
					t.Fatalf("will connect: %v", token.Error())
				}
				_ = socket.Close()
				waitFor(t, 5*time.Second, func() bool { return p.logs.count("will suppressed") == i+1 })
			}
			late := connect(t, p, "will-late", "bblp", accessCode, false)
			if got := late.subscribe(t, reportTopic(serial1), 0); got != 0 {
				t.Fatalf("late subscribe: %#x", got)
			}
			p1.publish(t, reportTopic(serial1), "real-report")
			waitFor(t, 5*time.Second, func() bool {
				return sub.box.has(reportTopic(serial1), "real-report") && late.box.has(reportTopic(serial1), "real-report")
			})
			for _, topic := range []string{reportTopic(serial1), requestTopic(serial1)} {
				if sub.box.has(topic, "SUPPRESSED-WILL") || late.box.has(topic, "SUPPRESSED-WILL") || p1.rec.count(topic, "SUPPRESSED-WILL") != 0 {
					t.Fatalf("will leaked on %s", topic)
				}
			}
		})
	}
}
