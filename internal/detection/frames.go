package detection

import (
	"context"
	"time"

	"bambu-mqtt-proxy/internal/camera"
)

// CameraFrames adapts the camera manager to the engine's frame source, so
// the engine core keeps working on plain frame values.
func CameraFrames(m *camera.Manager) FrameSource {
	return cameraFrames{m: m}
}

// cameraFrames is the FrameSource backed by the shared camera captures.
type cameraFrames struct {
	m *camera.Manager // shared capture owner
}

// Acquire starts (or joins) the shared capture for serial; false means the
// serial is unknown or its model cannot serve camera frames.
func (f cameraFrames) Acquire(serial string) bool {
	_, st := f.m.Acquire(serial)
	return st == camera.StatusOK
}

// Release drops one camera consumer interest taken with Acquire.
func (f cameraFrames) Release(serial string) {
	f.m.Release(serial)
}

// WaitFrame waits up to timeout for a frame newer than after.
func (f cameraFrames) WaitFrame(serial string, ctx context.Context, after uint64,
	timeout time.Duration) (Frame, bool) {
	frame := f.m.Wait(serial, ctx, after, timeout)
	if frame == nil {
		return Frame{}, false
	}
	return Frame{JPEG: frame.JPEG, Seq: frame.Seq, Captured: frame.Captured}, true
}

// IdleFrames is the frame source for blocked detection: the engine parks its
// workers before touching it, but the dependency stays non-nil.
type IdleFrames struct{}

// Acquire never starts a capture.
func (IdleFrames) Acquire(string) bool { return false }

// Release has nothing to release.
func (IdleFrames) Release(string) {}

// WaitFrame never returns a frame.
func (IdleFrames) WaitFrame(string, context.Context, uint64, time.Duration) (Frame, bool) {
	return Frame{}, false
}
