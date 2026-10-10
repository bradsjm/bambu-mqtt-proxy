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

// StateSource supplies the current job and session views in one coherent read.
type StateSource interface {
	// Print returns the preview-job and session projections for one serial.
	Print(serial string) (telemetry.JobView, telemetry.SessionView, bool)
}

// printSession tracks first-layer progress for one job generation.
type printSession struct {
	jobGen   uint64 // Job projection generation at the last sample.
	armed    bool   // A current-job layer 0 or 1 has been observed.
	recorded bool   // Completion has already been recorded for this job.
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

// sample reads the merged print-session state for serial once, advances the
// per-serial arming state against it, and returns the completion evidence the
// sample proved, if any. The evidence is copied from the very job that
// proved layer 2, so recording it later can never substitute newer identity
// from telemetry that switched prints in the meantime.
func (s *Service) sample(serial string, session *printSession) (activity.Observation, bool) {
	// Only live print sessions count: a print that has already finished or
	// failed gets its end-of-print entry instead, so a late "first layer
	// complete" would only mislead.
	job, view, ok := s.state.Print(serial)
	if !ok || !job.Active {
		return activity.Observation{}, false
	}
	// Arming follows the job generation: a replacement that is already past
	// layer 1 when first seen must not inherit the old arming and must
	// wait for its own layer-1 observation.
	if session.jobGen != job.Generation {
		*session = printSession{jobGen: job.Generation}
	}
	// Within a live job, reaching layer 2 proves the first layer is
	// done whether running or paused, so no gcode_state is read.
	layer := view.CurrentLayer()
	if layer == nil {
		return activity.Observation{}, false
	}
	if *layer <= 1 {
		session.armed = true
		return activity.Observation{}, false
	}
	if session.armed && !session.recorded {
		session.recorded = true
		return activity.Observation{JobGen: job.Generation}, true
	}
	return activity.Observation{}, false
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
			session := sessions[serial]
			if observation, ok := s.sample(serial, &session); ok {
				s.act.RecordObserved(serial, "first_layer_complete", activity.Info, "First layer complete", observation)
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
