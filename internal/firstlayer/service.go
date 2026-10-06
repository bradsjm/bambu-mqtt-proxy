// Package firstlayer records first-layer completion for print sessions observed from their first layer.
package firstlayer

import (
	"context"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/module"
	"bambu-mqtt-proxy/internal/telemetry"
)

// StateSource supplies current print-session state without changing report forwarding.
type StateSource interface {
	Session(serial string) (telemetry.SessionView, bool)
}

// printSession tracks arming and completion for one print generation.
type printSession struct {
	gen      uint64
	armed    bool
	recorded bool
}

// Service records first-layer completion without opening cameras or requesting reports.
type Service struct {
	serials []string           // configured printers, fixed at construction
	state   StateSource        // merged print-session state
	act     *activity.Log      // destination of the completion entries
	ctx     context.Context    // ends the observation loop
	cancel  context.CancelFunc // cancels ctx
	done    chan struct{}      // closed when the loop returns
}

// New builds a first-layer observer for the configured printers.
func New(printers []config.Printer, state StateSource, act *activity.Log) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	serials := make([]string, 0, len(printers))
	for _, p := range printers {
		serials = append(serials, p.Serial)
	}
	return &Service{
		serials: serials,
		state:   state,
		act:     act,
		ctx:     ctx,
		cancel:  cancel,
		done:    make(chan struct{}),
	}
}

// Start launches the observation goroutine. The core calls it once.
func (s *Service) Start() {
	go s.loop()
}

// Stop cancels observation and waits for the goroutine. The core calls it
// once, and only after Start.
func (s *Service) Stop() {
	s.cancel()
	<-s.done
}

// loop samples every printer at startup and every two seconds until stopped.
func (s *Service) loop() {
	defer close(s.done)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	sessions := make(map[string]printSession, len(s.serials))
	for {
		for _, serial := range s.serials {
			if s.ctx.Err() != nil {
				return
			}
			// Only live print sessions count: a print that has already
			// finished or failed gets its end-of-print entry instead, so a
			// late "first layer complete" would only mislead.
			view, known := s.state.Session(serial)
			if !known || !view.Active {
				continue
			}
			session := sessions[serial]
			if session.gen != view.SessionGen {
				session = printSession{gen: view.SessionGen}
			}
			// Within a live session, reaching layer 2 proves the first layer
			// is done whether running or paused, so no gcode_state is read.
			if view.LayerNum != nil {
				if *view.LayerNum <= 1 {
					session.armed = true
				} else if session.armed && !session.recorded {
					session.recorded = true
					s.act.Record(serial, "first_layer_complete", activity.Info, "First layer complete")
				}
			}
			sessions[serial] = session
		}
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Module declares the first-layer observer's lifecycle without report interest.
func (s *Service) Module() module.Module {
	return module.Module{
		Name:  "firstlayer",
		Start: s.Start,
		Stop:  s.Stop,
	}
}
