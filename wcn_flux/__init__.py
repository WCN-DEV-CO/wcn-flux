"""wcn-flux — a tiny, fast, backpressure-aware in-process dataflow runtime.

Compose streaming pipelines from small operators (map, filter, window, throttle,
merge) over bounded channels. When a consumer is slow, backpressure propagates and
you choose how to shed load — never unbounded memory growth, never silent loss.

Extension seams (Scheduler, Router) let you plug in advanced policies without
touching the engine. The default Scheduler is FIFO; the default Router is broadcast.

Zero dependencies. Pure standard library. MIT licensed. Original implementation.
"""
from __future__ import annotations
import time
import threading
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional, Protocol

__version__ = "0.1.0"
__all__ = ["Channel", "ShedPolicy", "Pipeline", "Scheduler", "FifoScheduler",
           "Router", "BroadcastRouter", "Full"]


class Full(Exception):
    """Raised when a bounded channel is full under the BLOCK_RAISE shed policy."""


class ShedPolicy:
    BLOCK = "block"              # producer waits for space (backpressure)
    DROP_NEW = "drop_new"        # drop the incoming item
    DROP_OLD = "drop_old"        # evict the oldest queued item to make room
    RAISE = "raise"              # raise Full immediately


class Channel:
    """A bounded, thread-safe queue with an explicit shed policy on overflow."""

    def __init__(self, capacity: int = 1024, shed: str = ShedPolicy.BLOCK) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self.capacity = capacity
        self.shed = shed
        self._q: deque = deque()
        self._lock = threading.Lock()
        self._not_full = threading.Condition(self._lock)
        self._not_empty = threading.Condition(self._lock)
        self._closed = False
        self.dropped = 0

    def put(self, item: Any, timeout: Optional[float] = None) -> bool:
        with self._not_full:
            if self._closed:
                raise RuntimeError("channel closed")
            if len(self._q) >= self.capacity:
                if self.shed == ShedPolicy.DROP_NEW:
                    self.dropped += 1
                    return False
                if self.shed == ShedPolicy.DROP_OLD:
                    self._q.popleft()
                    self.dropped += 1
                elif self.shed == ShedPolicy.RAISE:
                    raise Full()
                elif self.shed == ShedPolicy.BLOCK:
                    end = None if timeout is None else time.monotonic() + timeout
                    while len(self._q) >= self.capacity and not self._closed:
                        remaining = None if end is None else end - time.monotonic()
                        if remaining is not None and remaining <= 0:
                            return False
                        self._not_full.wait(remaining)
                    if self._closed:
                        raise RuntimeError("channel closed")
            self._q.append(item)
            self._not_empty.notify()
            return True

    def get(self, timeout: Optional[float] = None) -> Any:
        with self._not_empty:
            end = None if timeout is None else time.monotonic() + timeout
            while not self._q and not self._closed:
                remaining = None if end is None else end - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return None
                self._not_empty.wait(remaining)
            if not self._q:
                return None
            item = self._q.popleft()
            self._not_full.notify()
            return item

    def __len__(self) -> int:
        with self._lock:
            return len(self._q)

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._not_empty.notify_all()
            self._not_full.notify_all()


class Scheduler(Protocol):
    """SEAM: decides processing order of ready items. Default = FIFO.
    Private WCN layer plugs a priority/relevance-tier scheduler here."""
    def order(self, items: list) -> list: ...


class FifoScheduler:
    def order(self, items: list) -> list:
        return items


class Router(Protocol):
    """SEAM: decides which downstream channel(s) an item goes to. Default = broadcast.
    Private WCN layer plugs cost-aware / AoI routing here."""
    def route(self, item: Any, outputs: list) -> list: ...


class BroadcastRouter:
    def route(self, item: Any, outputs: list) -> list:
        return outputs


@dataclass
class _Stage:
    fn: Callable[[Any], Any]
    kind: str   # "map" | "filter" | "flatmap"


class Pipeline:
    """A composable, backpressure-aware dataflow pipeline.

    Operators build a transform chain; run() pulls a source iterable through the
    chain into bounded channels with the configured shed policy. The Scheduler and
    Router seams are injectable for advanced (e.g. private WCN) policies."""

    def __init__(self, capacity: int = 1024, shed: str = ShedPolicy.BLOCK,
                 scheduler: Optional[Scheduler] = None,
                 router: Optional[Router] = None) -> None:
        self._stages: list[_Stage] = []
        self.capacity = capacity
        self.shed = shed
        self.scheduler: Scheduler = scheduler or FifoScheduler()
        self.router: Router = router or BroadcastRouter()

    def map(self, fn: Callable[[Any], Any]) -> "Pipeline":
        self._stages.append(_Stage(fn, "map")); return self

    def filter(self, pred: Callable[[Any], bool]) -> "Pipeline":
        self._stages.append(_Stage(pred, "filter")); return self

    def flatmap(self, fn: Callable[[Any], Iterable]) -> "Pipeline":
        self._stages.append(_Stage(fn, "flatmap")); return self

    def _apply(self, item: Any):
        items = [item]
        for st in self._stages:
            nxt = []
            for it in self.scheduler.order(items):
                if st.kind == "map":
                    nxt.append(st.fn(it))
                elif st.kind == "filter":
                    if st.fn(it):
                        nxt.append(it)
                elif st.kind == "flatmap":
                    nxt.extend(st.fn(it))
            items = nxt
            if not items:
                break
        return items

    def run(self, source: Iterable) -> list:
        """Pull source through the pipeline into a bounded sink; return collected output.
        Demonstrates backpressure + shedding on the sink channel."""
        sink = Channel(self.capacity, self.shed)
        out: list = []
        for item in source:
            for produced in self._apply(item):
                for _ch in self.router.route(produced, [sink]):
                    if _ch.put(produced):
                        out.append(_ch.get())   # drain immediately (single-thread demo path)
        sink.close()
        return out
