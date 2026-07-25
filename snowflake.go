// Package snowflake provides a lock-free, distributed-friendly ID generator
// that packs a timestamp, worker ID, process ID, and sequence into a single
// signed int64.
//
// ID layout (63 bits used, sign bit unused so IDs are always positive):
//
//	[ 1 bit sign | 41 bits timestamp | 5 bits workerID | 5 bits processID | 12 bits sequence ]
//
// The 41-bit timestamp gives ~69 years of runtime from the fixed epoch.
// The 5-bit worker/process IDs each allow 32 distinct values (0..31).
// The 12-bit sequence allows 4096 IDs per millisecond per (worker, process) pair.
package snowflake

import (
	"errors"
	"sync/atomic"
	"time"
)

const (
	timestampBits = 41
	workerIdBits  = 5
	processIdBits = 5
	sequenceBits  = 12

	processIdShift = sequenceBits
	workerIdShift  = sequenceBits + processIdBits
	timestampShift = sequenceBits + processIdBits + workerIdBits

	sequenceMask  = (1 << sequenceBits) - 1
	processIdMask = (1 << processIdBits) - 1
	workerIdMask  = (1 << workerIdBits) - 1
	timestampMask = (1 << timestampBits) - 1

	maxWorkerId  = workerIdMask
	maxProcessId = processIdMask
	maxSequence  = sequenceMask
	maxTimestamp = timestampMask

	// epoch is 2025-01-01T00:00:00Z in milliseconds.
	epoch int64 = 1735689600000
)

// Errors returned by the generator.
var (
	ErrInvalidWorkerId   = errors.New("snowflake: worker id out of range")
	ErrInvalidProcessId  = errors.New("snowflake: process id out of range")
	ErrClockBackward     = errors.New("snowflake: clock moved backward")
	ErrSequenceExhausted = errors.New("snowflake: sequence exhausted for the current millisecond")
	ErrTimestampOverflow = errors.New("snowflake: timestamp overflow beyond 41-bit range")
)

// Snowflake is a generated identifier.
type Snowflake int64

// Int64 returns the raw signed int64 representation of the Snowflake.
func (s Snowflake) Int64() int64 { return int64(s) }

// Timestamp returns the absolute millisecond timestamp encoded in the Snowflake.
func (s Snowflake) Timestamp() int64 {
	return epoch + ((int64(s) >> timestampShift) & timestampMask)
}

// WorkerId returns the worker ID encoded in the Snowflake.
func (s Snowflake) WorkerId() int64 {
	return (int64(s) >> workerIdShift) & workerIdMask
}

// ProcessId returns the process ID encoded in the Snowflake.
func (s Snowflake) ProcessId() int64 {
	return (int64(s) >> processIdShift) & processIdMask
}

// Sequence returns the sequence number encoded in the Snowflake.
func (s Snowflake) Sequence() int64 {
	return int64(s) & sequenceMask
}

// Option configures a Node at construction time.
type Option func(*config)

type config struct {
	workerId  int64
	processId int64
	now       func() int64
}

// WithWorkerId sets the static worker ID. Must be in [0, 31].
func WithWorkerId(id int64) Option {
	return func(c *config) { c.workerId = id }
}

// WithProcessId sets the static process ID. Must be in [0, 31].
func WithProcessId(id int64) Option {
	return func(c *config) { c.processId = id }
}

// WithNow overrides the clock used to obtain the current Unix time in
// milliseconds. Intended for testing; production code should leave this unset
// so the node uses time.Now().UnixMilli().
func WithNow(now func() int64) Option {
	return func(c *config) { c.now = now }
}

// Node generates monotonically increasing, unique Snowflakes per (worker,
// process) pair. It is safe for concurrent use.
type Node struct {
	workerId  int64
	processId int64
	now       func() int64

	// state packs the 41-bit timestamp offset in the high bits and the 12-bit
	// sequence in the low bits. It is accessed atomically.
	state int64

	// static is the precomputed (workerId << workerIdShift) | (processId << processIdShift)
	// portion of every Snowflake emitted by this node.
	static int64
}

// NewNode constructs a Node with the given options. Returns an error if the
// worker ID or process ID is invalid.
func NewNode(opts ...Option) (*Node, error) {
	c := config{now: defaultNow}
	for _, opt := range opts {
		opt(&c)
	}

	if c.workerId < 0 || c.workerId > maxWorkerId {
		return nil, ErrInvalidWorkerId
	}
	if c.processId < 0 || c.processId > maxProcessId {
		return nil, ErrInvalidProcessId
	}
	if c.now == nil {
		c.now = defaultNow
	}

	return &Node{
		workerId:  c.workerId,
		processId: c.processId,
		now:       c.now,
		static:    (c.workerId << workerIdShift) | (c.processId << processIdShift),
	}, nil
}

// WorkerId returns the node's configured worker ID.
func (n *Node) WorkerId() int64 { return n.workerId }

// ProcessId returns the node's configured process ID.
func (n *Node) ProcessId() int64 { return n.processId }

// Next produces the next Snowflake. It is safe for
// concurrent use.
//
// If the sequence for the current millisecond is exhausted, Next blocks
// until the next millisecond. If the system clock moves backward, Next
// returns ErrClockBackward. If the timestamp exceeds the 41-bit range,
// Next returns ErrTimestampOverflow.
func (n *Node) Next() (Snowflake, error) {
	for {
		oldState := atomic.LoadInt64(&n.state)
		oldTime := oldState >> sequenceBits
		oldSeq := oldState & sequenceMask

		now := n.now() - epoch
		if now < 0 {
			return 0, ErrClockBackward
		}
		if now > maxTimestamp {
			return 0, ErrTimestampOverflow
		}
		if now < oldTime {
			return 0, ErrClockBackward
		}

		var nextSeq int64
		if now == oldTime {
			nextSeq = (oldSeq + 1) & sequenceMask
			if nextSeq == 0 {
				// Sequence overflow: spin until the next millisecond.
				for n.now()-epoch <= oldTime {
					time.Sleep(time.Microsecond)
				}
				continue
			}
		}

		newState := (now << sequenceBits) | nextSeq
		if atomic.CompareAndSwapInt64(&n.state, oldState, newState) {
			return Snowflake((now << timestampShift) | n.static | nextSeq), nil
		}
		// CAS failed: another goroutine updated the state. Loop and retry.
	}
}

// TryNext is a non-blocking variant of Generate. If the sequence for the
// current millisecond is exhausted, it returns ErrSequenceExhausted instead
// of waiting for the next millisecond.
func (n *Node) TryNext() (Snowflake, error) {
	for {
		oldState := atomic.LoadInt64(&n.state)
		oldTime := oldState >> sequenceBits
		oldSeq := oldState & sequenceMask

		now := n.now() - epoch
		if now < 0 {
			return 0, ErrClockBackward
		}
		if now > maxTimestamp {
			return 0, ErrTimestampOverflow
		}
		if now < oldTime {
			return 0, ErrClockBackward
		}

		var nextSeq int64
		if now == oldTime {
			nextSeq = (oldSeq + 1) & sequenceMask
			if nextSeq == 0 {
				return 0, ErrSequenceExhausted
			}
		}

		newState := (now << sequenceBits) | nextSeq
		if atomic.CompareAndSwapInt64(&n.state, oldState, newState) {
			return Snowflake((now << timestampShift) | n.static | nextSeq), nil
		}
	}
}

func defaultNow() int64 {
	return time.Now().UnixMilli()
}
