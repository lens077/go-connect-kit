package otelguard

// State is the circuit breaker state.
//
//	Closed ──failures reach threshold──> Open ──ProbeInterval──> HalfOpen
//	   ↑                                  │                        │
//	   └────────────success───────────────┴──────success───────────┘
//	                                      │
//	                                      └──probe failed──> Open (nextProbe reset)
//
// A failed probe keeps the circuit open without emitting another alert, so one
// outage produces exactly one alert and one recovery notice.
type State int

const (
	// StateClosed passes exports through to the inner exporter.
	StateClosed State = iota
	// StateOpen drops exports until ProbeInterval elapses.
	StateOpen
	// StateHalfOpen lets a single trial export through.
	StateHalfOpen
)

func (state State) String() string {
	switch state {
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}
