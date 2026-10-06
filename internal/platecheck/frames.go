package platecheck

import (
	"context"
	"time"

	"bambu-mqtt-proxy/internal/camera"
)

// captureTimeout bounds the complete wait for a post-trigger frame.
const captureTimeout = 10 * time.Second

// errCameraDisabled is the safe error returned by the idle adapter.
var errCameraDisabled = &safeError{code: "camera_unavailable"}

// Frame holds immutable bytes and exact capture metadata.
type Frame struct {
	// JPEG is the original camera JPEG.
	JPEG []byte
	// Seq is the shared capture sequence.
	Seq uint64
	// Captured is the shared capture timestamp.
	Captured time.Time
	// Width is the validated JPEG width.
	Width int
	// Height is the validated JPEG height.
	Height int
}

// FrameSource captures an image strictly newer than the trigger timestamp.
type FrameSource interface {
	// Capture returns an immutable post-trigger JPEG with its metadata.
	Capture(context.Context, string, time.Time) (Frame, error)
}

// webFrames is the package-local camera lease boundary used by focused tests.
type webFrames interface {
	// AcquireWeb takes a shared web-camera consumer reference.
	AcquireWeb(string) (chan struct{}, camera.Status)
	// Release balances one successful acquisition.
	Release(string)
	// Latest returns the newest shared frame without waiting.
	Latest(string) *camera.Frame
	// Wait takes a temporary reference while waiting for a newer sequence.
	Wait(string, context.Context, uint64, time.Duration) *camera.Frame
}

// cameraFrames adapts the shared web camera capture without altering its bytes.
type cameraFrames struct {
	// m owns shared web captures and their consumer references.
	m webFrames
}

// CameraFrames constructs the post-trigger web capture adapter.
func CameraFrames(m *camera.Manager) FrameSource { return cameraFrames{m: m} }

// Capture holds one web lease until a strictly post-trigger frame is available.
func (f cameraFrames) Capture(ctx context.Context, serial string, after time.Time) (Frame, error) {
	ctx, cancel := context.WithTimeout(ctx, captureTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		code := "capture_timeout"
		if err == context.Canceled {
			code = "canceled"
		}
		return Frame{}, &safeError{code: code}
	}
	_, status := f.m.AcquireWeb(serial)
	if status != camera.StatusOK {
		code := "camera_unavailable"
		if status == camera.StatusUnsupportedModel {
			code = "unsupported_camera"
		}
		return Frame{}, &safeError{code: code}
	}
	defer f.m.Release(serial)
	var seq uint64
	current := f.m.Latest(serial)
	for {
		if current != nil {
			if current.Captured.After(after) {
				width, height, err := imageDimensions(current.JPEG)
				if err != nil {
					return Frame{}, err
				}
				return Frame{JPEG: current.JPEG, Seq: current.Seq, Captured: current.Captured, Width: width, Height: height}, nil
			}
			seq = current.Seq
		}
		current = f.m.Wait(serial, ctx, seq, captureTimeout)
		if current == nil {
			if ctx.Err() == context.Canceled {
				return Frame{}, &safeError{code: "canceled"}
			}
			return Frame{}, &safeError{code: "capture_timeout"}
		}
		if err := ctx.Err(); err != nil {
			code := "capture_timeout"
			if err == context.Canceled {
				code = "canceled"
			}
			return Frame{}, &safeError{code: code}
		}
	}
}

// IdleFrames keeps the service dependency non-nil while cameras are disabled.
type IdleFrames struct{}

// Capture reports camera unavailability without opening a connection.
func (IdleFrames) Capture(context.Context, string, time.Time) (Frame, error) {
	return Frame{}, errCameraDisabled
}
