# wcn-flux

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Zero dependencies](https://img.shields.io/badge/dependencies-0-brightgreen.svg)](#)
[![Python 3.8+](https://img.shields.io/badge/python-3.8%2B-blue.svg)](#)

A tiny, fast, **backpressure-aware in-process dataflow runtime**. Compose streaming
pipelines from small operators over **bounded** channels — when a consumer is slow,
backpressure propagates and you decide how to shed load. No unbounded memory growth,
no silent loss. Zero dependencies.

## Why
Most "stream processing" toolkits are huge, or quietly grow memory until they OOM.
`wcn-flux` is small, explicit about overflow, and built around clean extension seams.

## Install
```bash
pip install wcn-flux
```

## Quick start
```python
from wcn_flux import Pipeline

out = (Pipeline()
       .map(lambda x: x * 2)
       .filter(lambda x: x > 4)
       .run([1, 2, 3, 4]))
# -> [6, 8]
```

## Bounded channels + shed policies
```python
from wcn_flux import Channel, ShedPolicy

ch = Channel(capacity=1024, shed=ShedPolicy.BLOCK)      # backpressure (default)
# also: DROP_NEW, DROP_OLD, RAISE  — you choose how overflow behaves
ch.put(item, timeout=0.5)   # False if it couldn't make space in time
print(ch.dropped)           # count of shed items
```

## Extension seams
`wcn-flux` ships sensible defaults but exposes two seams so you can plug in advanced
policies without forking the engine:

```python
from wcn_flux import Pipeline, FifoScheduler, BroadcastRouter

class PriorityScheduler(FifoScheduler):
    def order(self, items):           # reorder ready items however you like
        return sorted(items, key=my_priority)

Pipeline(scheduler=PriorityScheduler())   # Router seam works the same way
```

- **Scheduler** — decides processing order of ready items (default: FIFO)
- **Router** — decides which downstream channel(s) an item goes to (default: broadcast)

This is what makes `wcn-flux` an *engine*: bring your own routing/scheduling brain.

## Features
- ✅ Bounded channels with 4 explicit shed policies (block / drop-new / drop-old / raise)
- ✅ Real backpressure (blocking producer unblocks when drained)
- ✅ `map` / `filter` / `flatmap` operators, chainable
- ✅ Thread-safe channels (conditions, no busy-wait)
- ✅ Pluggable Scheduler + Router seams
- ✅ **Zero dependencies**

## License
MIT © WCN Development Co
