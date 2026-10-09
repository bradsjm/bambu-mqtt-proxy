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
	// Job supplies the job projection of the same report stream, so
	// events can carry original job evidence beside session evidence.
	Job(serial string) (telemetry.JobView, bool)
}

// printSession tracks first-layer progress for one job/session generation pair.
type printSession struct {
	jobGen     uint64 // Job projection generation at the last sample.
	sessionGen uint64 // Session projection generation at the last sample.
	armed      bool   // A current-job layer 0 or 1 has been observed.
	recorded   bool   // Completion has already been recorded for this pair.
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

// sampleSnapshot reads one coherent Job+Session pair for serial. Each
// projection is read twice with the other in between and compared with
// itself, so a replacement that happens between the reads is retried
// instead of combined; three bounded attempts reject an incoherent source.
// The Job and Session projections use separate clocks, so their timestamps
// are only ever compared with the same projection's own re-read. The ok
// result is false for unknown serials and for a source that never settled.
func (s *Service) sampleSnapshot(serial string) (telemetry.JobView, telemetry.SessionView, bool) {
	for range 3 {
		firstJob, ok := s.state.Job(serial)
		if !ok {
			return telemetry.JobView{}, telemetry.SessionView{}, false
		}
		firstSession, ok := s.state.Session(serial)
		if !ok {
			return telemetry.JobView{}, telemetry.SessionView{}, false
		}
		job, ok := s.state.Job(serial)
		if !ok {
			return telemetry.JobView{}, telemetry.SessionView{}, false
		}
		session, ok := s.state.Session(serial)
		if !ok {
			return telemetry.JobView{}, telemetry.SessionView{}, false
		}
		jobStable := firstJob.Generation == job.Generation && firstJob.Revision == job.Revision &&
			firstJob.RunningEpoch == job.RunningEpoch && firstJob.Active == job.Active &&
			firstJob.State == job.State && firstJob.StateGen == job.StateGen &&
			firstJob.ObsGen == job.ObsGen && firstJob.ObsAt.Equal(job.ObsAt)
		sessionStable := firstSession.SessionGen == session.SessionGen && firstSession.Epoch == session.Epoch &&
			firstSession.StateGen == session.StateGen && firstSession.Obs == session.Obs &&
			firstSession.ObsGen == session.ObsGen && firstSession.ObsAt.Equal(session.ObsAt) &&
			firstSession.State == session.State && firstSession.Active == session.Active &&
			(firstSession.LayerNum == nil) == (session.LayerNum == nil) &&
			(firstSession.LayerNum == nil || *firstSession.LayerNum == *session.LayerNum)
		// Both projections stamp the last real print report with the same
		// transport generation, so equal ObsGen marks the pair as taken
		// from one report stream. State agreement is not required: a
		// stateless report that starts a new job keeps the session's
		// state while the job projection clears into the new generation.
		coherent := job.Active && job.ObsGen == session.ObsGen
		if jobStable && sessionStable && coherent {
			return job, session, true
		}
	}
	return telemetry.JobView{}, telemetry.SessionView{}, false
}

// sample reads the merged print-session state for serial once, advances the
// per-serial arming state against it, and returns the completion evidence the
// sample proved, if any. The evidence is copied from the very job/session
// pair that proved layer 2, so recording it later can never substitute newer
// identity from telemetry that switched prints in the meantime.
func (s *Service) sample(serial string, session *printSession) (activity.Observation, bool) {
	// Only live print sessions count: a print that has already finished or
	// failed gets its end-of-print entry instead, so a late "first layer
	// complete" would only mislead.
	job, view, ok := s.sampleSnapshot(serial)
	if !ok || !view.Active {
		return activity.Observation{}, false
	}
	// Arming follows the job/session pair: either projection changing
	// means the observed print is a different one, so a replacement that
	// is already past layer 1 when first seen must not inherit the old
	// arming and must wait for its own layer-1 observation.
	if session.jobGen != job.Generation || session.sessionGen != view.SessionGen {
		*session = printSession{jobGen: job.Generation, sessionGen: view.SessionGen}
	}
	// Within a live session, reaching layer 2 proves the first layer is
	// done whether running or paused, so no gcode_state is read.
	if view.LayerNum != nil {
		if *view.LayerNum <= 1 {
			session.armed = true
		} else if session.armed && !session.recorded {
			session.recorded = true
			return activity.Observation{
				JobGen:       job.Generation,
				JobRevision:  job.Revision,
				RunningEpoch: job.RunningEpoch,
				SessionGen:   view.SessionGen,
				Epoch:        view.Epoch,
				StateGen:     view.StateGen,
				ObsGen:       view.ObsGen,
				ObsAt:        view.ObsAt,
			}, true
		}
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
