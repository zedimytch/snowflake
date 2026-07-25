# snowflake

A lock-free, distributed-friendly Snowflake ID generator for Go. IDs are
packed into a single signed `int64`:

```
| 1 bit sign (unused) | 41 bits timestamp | 5 bits workerID | 5 bits processID | 12 bits sequence |
```

- 41-bit timestamp: milliseconds since a fixed epoch (2025-01-01T00:00:00Z),
  giving ~69 years of runtime.
- 5-bit worker ID: 0..31.
- 5-bit process ID: 0..31.
- 12-bit sequence: 4096 IDs per millisecond per (worker, process) pair.

The generator is safe for concurrent use.

## Install

```
go get github.com/zedimytch/snowflake
```

## Usage

```go
package main

import (
	"fmt"
	"github.com/zedimytch/snowflake"
)

func main() {
	node, err := snowflake.NewNode(
		snowflake.WithWorkerId(1),
		snowflake.WithProcessId(2),
	)
	if err != nil {
		panic(err)
	}

	id, err := node.Next()
	if err != nil {
		panic(err)
	}

	fmt.Printf("id         = %d\n", id.Int64())
	fmt.Printf("timestamp  = %d\n", id.Timestamp())
	fmt.Printf("worker id  = %d\n", id.WorkerId())
	fmt.Printf("process id = %d\n", id.ProcessId())
	fmt.Printf("sequence   = %d\n", id.Sequence())
}
```
