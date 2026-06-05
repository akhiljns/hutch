package hutch

// ErrorPolicy decides what happens to a message when its handler returns an
// error.
type ErrorPolicy int

const (
	// Drop acknowledges the failed message, removing it (at-most-once). This is
	// the default: it never lets failures accumulate as unacked messages or
	// flood a dead-letter queue. Pair it with OnErrorFunc to capture failures.
	Drop ErrorPolicy = iota

	// Requeue negatively-acknowledges with redelivery. Use only when failures
	// are expected to be transient — a permanently-failing ("poison") message
	// will loop forever, so add your own attempt cap if you choose this.
	Requeue

	// Reject negatively-acknowledges without redelivery. On brokers with a
	// dead-letter destination configured for the queue the message is routed
	// there; otherwise it is dropped.
	Reject
)

func (p ErrorPolicy) String() string {
	switch p {
	case Drop:
		return "drop"
	case Requeue:
		return "requeue"
	case Reject:
		return "reject"
	default:
		return "unknown"
	}
}
