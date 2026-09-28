// Package camera implements Bambu chamber image and RTSPS camera capture,
// then serves the captured JPEG frames over HTTP.
//
// The protocol is a TLS session on printer port 6000: the client sends an
// 80-byte authentication payload (username bblp plus the LAN access code)
// and the printer then answers with length-prefixed JPEG frames
// (16-byte header, little-endian uint32 payload length first).
package camera

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// Camera protocol framing constants.
const (
	// Port is the chamber image camera port on P1 and A1 series printers.
	Port = 6000
	// cameraAuthMagic and cameraAuthCommand open every authentication
	// payload.
	cameraAuthMagic   = 0x40
	cameraAuthCommand = 0x3000
	// authPayloadLen is the fixed authentication payload length.
	authPayloadLen = 80
	// frameHeaderLen is the per-frame header length; its first little-endian
	// uint32 carries the JPEG payload length.
	frameHeaderLen = 16
	// maxPayloadLen caps the declared frame payload before allocation.
	maxPayloadLen = 10 << 20 // 10 MiB
)

// authPayload builds the fixed-length camera authentication payload: magic
// 0x40, command 0x3000, padding, the username and the access code, each
// null-padded to 32 bytes.
func authPayload(username, accessCode string) []byte {
	p := make([]byte, authPayloadLen)
	binary.LittleEndian.PutUint32(p[0:4], cameraAuthMagic)
	binary.LittleEndian.PutUint32(p[4:8], cameraAuthCommand)
	copy(p[16:48], username)
	copy(p[48:80], accessCode)
	return p
}

// validAuthHeader reports whether a received payload opens with the camera
// authentication magic and command.
func validAuthHeader(payload []byte) bool {
	return len(payload) >= 8 &&
		binary.LittleEndian.Uint32(payload[0:4]) == cameraAuthMagic &&
		binary.LittleEndian.Uint32(payload[4:8]) == cameraAuthCommand
}

// cameraAddress derives the camera endpoint from the printer MQTT address
// host. The chamber image camera always serves on port 6000 regardless of
// the MQTT endpoint's port (which may be a nonstandard relay port).
func cameraAddress(printer config.Printer) (string, error) {
	host, _, err := net.SplitHostPort(printer.Address)
	if err != nil {
		return "", fmt.Errorf("printer address %q: %w", printer.Address, err)
	}
	return net.JoinHostPort(host, fmt.Sprint(Port)), nil
}

// dialTLS opens the camera TLS session. Printer certificates are
// self-signed, so verification is disabled like every other Bambu hop.
func dialTLS(ctx context.Context, address string) (net.Conn, error) {
	d := net.Dialer{Timeout: 10 * time.Second}
	raw, err := d.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	cfg := tls.Config{InsecureSkipVerify: true} // self-signed printer cert
	conn := tls.Client(raw, &cfg)
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := conn.HandshakeContext(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("camera handshake: %w", err)
	}
	return conn, nil
}

// authenticate sends the auth payload and consumes the printer's answer.
func authenticate(w io.Writer, r *bufio.Reader, username, accessCode string) error {
	if _, err := w.Write(authPayload(username, accessCode)); err != nil {
		return fmt.Errorf("send camera auth: %w", err)
	}
	// No login reply exists: the printer answers the auth payload directly
	// with its first 16-byte-header JPEG frame (verified against the
	// bambuddy reference implementation, read_chamber_image_frame).
	return nil
}

// readFrame reads one raw frame: a 16-byte header whose first little-endian
// uint32 is the payload length, then the JPEG payload. The header bytes are
// returned untouched so raw camera sessions can replay them verbatim; JPEG
// start and end markers are validated so partial TLS reads never reach
// clients.
func readFrame(r *bufio.Reader) (*Frame, error) {
	var header [frameHeaderLen]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, fmt.Errorf("read camera frame header: %w", err)
	}
	length := binary.LittleEndian.Uint32(header[0:4])
	if length == 0 || length > maxPayloadLen {
		return nil, fmt.Errorf("camera frame length %d out of range", length)
	}
	jpeg := make([]byte, length)
	if _, err := io.ReadFull(r, jpeg); err != nil {
		return nil, fmt.Errorf("read camera frame: %w", err)
	}
	if !isJPEG(jpeg) {
		return nil, fmt.Errorf("camera frame is not JPEG")
	}
	raw := make([]byte, frameHeaderLen)
	copy(raw, header[:])
	return &Frame{JPEG: jpeg, Header: raw}, nil
}

// isJPEG validates the SOI and EOI markers that bracket every JPEG.
func isJPEG(frame []byte) bool {
	return len(frame) >= 4 && frame[0] == 0xFF && frame[1] == 0xD8 &&
		frame[len(frame)-2] == 0xFF && frame[len(frame)-1] == 0xD9
}
