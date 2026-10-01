package notification

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"bambu-mqtt-proxy/internal/config"
)

// pointPushoverURL aims pushoverURL at srv for the test and restores it.
func pointPushoverURL(t *testing.T, srv *httptest.Server) {
	t.Helper()
	orig := pushoverURL
	pushoverURL = srv.URL
	t.Cleanup(func() { pushoverURL = orig })
}

// TestSendPostsMultipartMessage pins the request shape: trimmed
// credentials, rune-limited title and message, and the JPEG attachment
// part.
func TestSendPostsMultipartMessage(t *testing.T) {
	var gotFields map[string]string
	var gotAttachment []byte
	var gotDisposition, gotType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Errorf("ParseMultipartForm: %v", err)
			return
		}
		gotFields = map[string]string{}
		for _, k := range []string{"token", "user", "title", "message"} {
			gotFields[k] = r.FormValue(k)
		}
		file, header, err := r.FormFile("attachment")
		if err != nil {
			t.Errorf("FormFile(attachment): %v", err)
			return
		}
		defer file.Close()
		gotDisposition = header.Header.Get("Content-Disposition")
		gotType = header.Header.Get("Content-Type")
		if gotAttachment, err = io.ReadAll(file); err != nil {
			t.Errorf("read attachment: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]int{"status": 1})
	}))
	defer srv.Close()
	pointPushoverURL(t, srv)

	cfg := config.Notifications{
		Enabled:  true,
		Provider: "pushover",
		Pushover: config.Pushover{AppToken: " app-token ", UserKey: " user-key "},
	}
	longTitle := strings.Repeat("t", 300)
	longBody := strings.Repeat("m", 2000)
	jpeg := []byte{0xFF, 0xD8, 0xFF, 0xE0, 1, 2, 3}
	if err := Send(context.Background(), cfg, longTitle, longBody, jpeg); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if gotFields["token"] != "app-token" || gotFields["user"] != "user-key" {
		t.Fatalf("credentials = %+v", gotFields)
	}
	if gotFields["title"] != strings.Repeat("t", maxTitleRunes) {
		t.Fatalf("title length = %d, want %d", len([]rune(gotFields["title"])), maxTitleRunes)
	}
	if gotFields["message"] != strings.Repeat("m", maxMessageRunes) {
		t.Fatalf("message length = %d, want %d", len([]rune(gotFields["message"])), maxMessageRunes)
	}
	if gotDisposition != `form-data; name="attachment"; filename="snapshot.jpg"` || gotType != "image/jpeg" {
		t.Fatalf("attachment header = %q %q", gotDisposition, gotType)
	}
	if string(gotAttachment) != string(jpeg) {
		t.Fatalf("attachment = %v, want %v", gotAttachment, jpeg)
	}
}

// TestSendStatusZeroReturnsErrorWithoutCredentials pins that a JSON
// rejection surfaces a safe error that never echoes either credential.
func TestSendStatusZeroReturnsErrorWithoutCredentials(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]int{"status": 0})
	}))
	defer srv.Close()
	pointPushoverURL(t, srv)

	cfg := config.Notifications{Provider: "pushover",
		Pushover: config.Pushover{AppToken: "secret-token", UserKey: "secret-user"}}
	err := Send(context.Background(), cfg, "title", "body", nil)
	if err == nil {
		t.Fatal("status 0 accepted")
	}
	for _, secret := range []string{"secret-token", "secret-user"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error leaks credentials: %v", err)
		}
	}
}

// TestSendHTTPErrorReturnsSafeError pins the non-200 wording and that no
// response body reaches the caller.
func TestSendHTTPErrorReturnsSafeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "secret-response-body", http.StatusBadGateway)
	}))
	defer srv.Close()
	pointPushoverURL(t, srv)

	cfg := config.Notifications{Provider: "pushover",
		Pushover: config.Pushover{AppToken: "secret-token", UserKey: "secret-user"}}
	err := Send(context.Background(), cfg, "title", "body", []byte{1})
	if err == nil || err.Error() != "pushover rejected the notification (HTTP 502)" {
		t.Fatalf("Send error = %v", err)
	}
	if strings.Contains(err.Error(), "secret-response-body") {
		t.Fatalf("error leaks the response body: %v", err)
	}
}
