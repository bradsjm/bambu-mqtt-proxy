package jobpreview

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"path"
	"slices"
	"strings"
	"time"

	"bambu-mqtt-proxy/internal/config"
	"github.com/jlaffaye/ftp"
)

// readCap bounds each archive download and the total extracted bytes.
const readCap = int64(64 << 20)

// tlsDial returns a context-bound implicit TLS dialer with one absolute deadline.
func tlsDial(ctx context.Context, deadline time.Time, cfg *tls.Config) func(string, string) (net.Conn, error) {
	return func(network, address string) (net.Conn, error) {
		raw, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		if err := raw.SetDeadline(deadline); err != nil {
			raw.Close()
			return nil, err
		}
		context.AfterFunc(ctx, func() { raw.Close() })
		return tls.Client(raw, cfg), nil
	}
}

// fetch retrieves the first available candidate archive over protected FTPS.
func fetch(ctx context.Context, p config.Printer, k jobKey) (Result, error) {
	host, _, err := net.SplitHostPort(p.Address)
	if err != nil {
		return Result{}, err
	}
	return fetchAddress(ctx, net.JoinHostPort(host, "990"), p, k)
}

// fetchAddress connects to an implicit TLS server and extracts its archived preview.
func fetchAddress(ctx context.Context, address string, p config.Printer, k jobKey) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	deadline, _ := ctx.Deadline()
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return Result{}, err
	}
	cfg := &tls.Config{ServerName: host, InsecureSkipVerify: p.InsecureSkipVerify, ClientSessionCache: tls.NewLRUClientSessionCache(1), MinVersion: tls.VersionTLS12}
	conn, err := ftp.Dial(address, ftp.DialWithTLS(cfg), ftp.DialWithDialFunc(tlsDial(ctx, deadline, cfg)))
	if err != nil {
		return Result{}, err
	}
	defer conn.Quit()
	if err := conn.Login(p.Username, p.Password); err != nil {
		return Result{}, err
	}
	probes := 0
	for _, dir := range []string{"/cache", "/"} {
		for _, name := range candidates(k) {
			remote := path.Join(dir, name)
			probes++
			size, err := conn.FileSize(remote)
			if err != nil {
				continue
			}
			if size <= 0 || size > readCap {
				return Result{}, fmt.Errorf("archive size outside read cap")
			}
			reader, err := conn.Retr(remote)
			if err != nil {
				return Result{}, err
			}
			data, err := io.ReadAll(io.LimitReader(reader, readCap+1))
			closeErr := reader.Close()
			if err != nil {
				return Result{}, err
			}
			if closeErr != nil {
				return Result{}, closeErr
			}
			if int64(len(data)) > readCap {
				return Result{}, fmt.Errorf("archive exceeds read cap")
			}
			result, err := parseArchive(data, k)
			result.ArchiveBytes = len(data)
			result.RemotePath = remote
			result.Probes = probes
			return result, err
		}
	}
	return Result{}, fmt.Errorf("archive not found")
}

// candidates derives unique archive basenames without FTP command delimiters.
func candidates(k jobKey) []string {
	var names []string
	add := func(name string) {
		if name != "" && name != "." && !strings.ContainsAny(name, "\r\n\x00/\\") && !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	file := path.Base(strings.ReplaceAll(k.File, "\\", "/"))
	if strings.HasSuffix(strings.ToLower(file), ".3mf") {
		add(file)
	}
	if strings.HasSuffix(strings.ToLower(k.Name), ".3mf") {
		add(k.Name)
	} else if k.Name != "" {
		add(k.Name + ".3mf")
		add(k.Name + ".gcode.3mf")
	}
	return names
}
