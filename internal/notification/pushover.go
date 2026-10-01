// Package notification delivers print notifications to one external
// service. Pushover is the only provider today; Send dispatches on the
// configured provider, so another service needs one new case there.
package notification

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// Pushover message limits, https://pushover.net/api#limits.
const (
	maxTitleRunes   = 250
	maxMessageRunes = 1024
	// maxSnapshotSize is the largest accepted JPEG attachment in bytes.
	maxSnapshotSize = 5_242_880
)

// pushoverURL is the Pushover message endpoint; tests point it at a fake
// server.
var pushoverURL = "https://api.pushover.net/1/messages.json"

// pushoverClient posts one message; the timeout bounds the whole exchange.
// Notifications are rare and best-effort, so there is no retry.
var pushoverClient = &http.Client{Timeout: 15 * time.Second}

// Send delivers one message through the configured notification provider.
// An empty provider means pushover. A nonempty jpeg attaches one camera
// snapshot. Errors are safe: they never contain credentials or the
// response body.
func Send(ctx context.Context, cfg config.Notifications, title, body string, jpeg []byte) error {
	switch cfg.Provider {
	case "", "pushover":
	default:
		return fmt.Errorf("unsupported notification provider %q", cfg.Provider)
	}

	buf := &bytes.Buffer{}
	form := multipart.NewWriter(buf)
	for _, f := range []struct{ name, value string }{
		{"token", strings.TrimSpace(cfg.Pushover.AppToken)},
		{"user", strings.TrimSpace(cfg.Pushover.UserKey)},
		{"title", truncateRunes(title, maxTitleRunes)},
		{"message", truncateRunes(body, maxMessageRunes)},
	} {
		if err := form.WriteField(f.name, f.value); err != nil {
			return fmt.Errorf("build pushover form: %w", err)
		}
	}
	if 0 < len(jpeg) && len(jpeg) <= maxSnapshotSize {
		h := textproto.MIMEHeader{}
		h.Set("Content-Disposition", `form-data; name="attachment"; filename="snapshot.jpg"`)
		h.Set("Content-Type", "image/jpeg")
		part, err := form.CreatePart(h)
		if err != nil {
			return fmt.Errorf("build pushover attachment: %w", err)
		}
		if _, err := part.Write(jpeg); err != nil {
			return fmt.Errorf("write pushover attachment: %w", err)
		}
	}
	if err := form.Close(); err != nil {
		return fmt.Errorf("build pushover form: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, pushoverURL, buf)
	if err != nil {
		return fmt.Errorf("build pushover request: %w", err)
	}
	req.Header.Set("Content-Type", form.FormDataContentType())
	res, err := pushoverClient.Do(req)
	if err != nil {
		return fmt.Errorf("pushover request: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 64<<10))
		return fmt.Errorf("pushover rejected the notification (HTTP %d)", res.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	if err != nil {
		return fmt.Errorf("pushover did not accept the notification")
	}
	var reply struct {
		Status int `json:"status"`
	}
	if err := json.Unmarshal(raw, &reply); err != nil || reply.Status != 1 {
		return fmt.Errorf("pushover did not accept the notification")
	}
	return nil
}

// truncateRunes cuts s to at most n runes without splitting one.
func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}
