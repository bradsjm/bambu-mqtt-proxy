package platecheck

import (
	"context"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/camera"
)

type fakeWeb struct {
	status             camera.Status
	latest             *camera.Frame
	frames             []*camera.Frame
	acquired, released int
	after              []uint64
	deadlines          []time.Time
	onWait             func(context.Context)
}

func (f *fakeWeb) AcquireWeb(string) (chan struct{}, camera.Status) {
	f.acquired++
	return nil, f.status
}
func (f *fakeWeb) Release(string)              { f.released++ }
func (f *fakeWeb) Latest(string) *camera.Frame { return f.latest }
func (f *fakeWeb) Wait(_ string, ctx context.Context, after uint64, _ time.Duration) *camera.Frame {
	f.after = append(f.after, after)
	deadline, _ := ctx.Deadline()
	f.deadlines = append(f.deadlines, deadline)
	if f.onWait != nil {
		f.onWait(ctx)
	}
	if len(f.frames) == 0 {
		return nil
	}
	v := f.frames[0]
	f.frames = f.frames[1:]
	return v
}
func TestCameraFramesStrictPostTriggerAndImmutableBytes(t *testing.T) {
	data := testJPEG(t)
	boundary := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	f := &fakeWeb{latest: &camera.Frame{JPEG: data, Seq: 5, Captured: boundary.Add(-5 * time.Second)}, frames: []*camera.Frame{
		{JPEG: data, Seq: 6, Captured: boundary}, {JPEG: data, Seq: 7, Captured: boundary.Add(time.Millisecond)},
	}}
	frame, err := (cameraFrames{m: f}).Capture(context.Background(), "01S1", boundary)
	if err != nil || frame.Seq != 7 || frame.Width != 8 || frame.Height != 6 || &frame.JPEG[0] != &data[0] {
		t.Fatalf("frame %+v err %v", frame, err)
	}
	if f.acquired != 1 || f.released != 1 || len(f.after) != 2 || f.after[0] != 5 || f.after[1] != 6 {
		t.Fatalf("leases/waits %+v", f)
	}
	if !f.deadlines[0].Equal(f.deadlines[1]) {
		t.Fatal("deadline reset per frame")
	}
}
func TestCameraFramesFreshLatestNeedsNoWait(t *testing.T) {
	boundary := time.Now()
	f := &fakeWeb{latest: &camera.Frame{JPEG: testJPEG(t), Seq: 7, Captured: boundary.Add(time.Millisecond)}}
	frame, err := (cameraFrames{m: f}).Capture(context.Background(), "01S1", boundary)
	if err != nil || frame.Seq != 7 || len(f.after) != 0 || f.released != 1 {
		t.Fatalf("fresh latest %+v %v", frame, err)
	}
}
func TestCameraFramesFailureAndCancellationBalanceLeases(t *testing.T) {
	for _, tc := range []struct {
		status  camera.Status
		code    string
		release int
	}{
		{camera.StatusUnsupportedModel, "unsupported_camera", 0}, {camera.StatusUnavailable, "camera_unavailable", 0}, {camera.StatusUnknownSerial, "camera_unavailable", 0}, {camera.StatusOK, "capture_timeout", 1},
	} {
		f := &fakeWeb{status: tc.status}
		_, err := (cameraFrames{m: f}).Capture(context.Background(), "01S1", time.Now())
		if err == nil || errorCategory(err) != tc.code || f.released != tc.release {
			t.Fatalf("status %v error %v release %d", tc.status, err, f.released)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	f := &fakeWeb{onWait: func(context.Context) { cancel() }}
	_, err := (cameraFrames{m: f}).Capture(ctx, "01S1", time.Now())
	if err == nil || errorCategory(err) != "canceled" || f.released != 1 {
		t.Fatalf("cancel %v release %d", err, f.released)
	}
	_, err = (IdleFrames{}).Capture(context.Background(), "01S1", time.Now())
	if err != errCameraDisabled {
		t.Fatalf("idle %v", err)
	}
}
