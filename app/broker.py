"""Task queue abstraction.

Two implementations behind one interface:

- `RabbitMQBroker` — the real thing, durable queue with manual acks.
- `InMemoryBroker` — an in-process deque, selected with `BROKER_URL=memory://`.
  It lets the test suite exercise the whole publish/consume/ack path without a
  running RabbitMQ, and keeps the worker logic identical in both cases.

Messages are small: the task's identity, not its state. The worker re-reads the
task row from Postgres, so a stale or duplicated message can never resurrect an
out-of-date view of the task.
"""

from __future__ import annotations

import heapq
import json
import logging
import math
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from app import config

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TaskMessage:
    """What travels on the queue."""

    task_id: str
    run_id: str
    task_name: str
    attempt: int = 0
    reason: str | None = None  # why it was dead-lettered, if it was

    def to_json(self) -> bytes:
        payload = {
            "task_id": self.task_id,
            "run_id": self.run_id,
            "task_name": self.task_name,
            "attempt": self.attempt,
        }
        if self.reason is not None:
            payload["reason"] = self.reason
        return json.dumps(payload).encode()

    @classmethod
    def from_json(cls, raw: bytes) -> "TaskMessage":
        data = json.loads(raw)
        return cls(
            task_id=data["task_id"],
            run_id=data["run_id"],
            task_name=data["task_name"],
            attempt=data.get("attempt", 0),
            reason=data.get("reason"),
        )


# The worker hands back True to ack, False to nack. Phase 3 uses the nack path
# for retries; Phase 2 always acks, since a failed task is a recorded outcome,
# not a delivery failure.
Handler = Callable[[TaskMessage], bool]


class Broker(Protocol):
    def publish(self, message: TaskMessage, delay: float = 0.0) -> None: ...

    def dead_letter(self, message: TaskMessage) -> None: ...

    def consume(self, handler: Handler) -> None: ...

    def close(self) -> None: ...


class InMemoryBroker:
    """In-process queue. Not durable, not cross-process — tests and demos only.

    Delays are honoured against the wall clock, so the retry path behaves the same
    way it does on RabbitMQ, just without the broker.
    """

    def __init__(self) -> None:
        self._heap: list[tuple[float, int, TaskMessage]] = []
        self._dead: list[TaskMessage] = []
        self._counter = 0
        self._lock = threading.Lock()

    def publish(self, message: TaskMessage, delay: float = 0.0) -> None:
        with self._lock:
            self._counter += 1
            heapq.heappush(
                self._heap, (time.monotonic() + delay, self._counter, message)
            )
        logger.debug("published %s (delay %.3fs)", message.task_name, delay)

    def dead_letter(self, message: TaskMessage) -> None:
        with self._lock:
            self._dead.append(message)
        logger.warning("dead-lettered %s: %s", message.task_name, message.reason)

    def _pop_due(self) -> TaskMessage | None:
        with self._lock:
            if self._heap and self._heap[0][0] <= time.monotonic():
                return heapq.heappop(self._heap)[2]
            return None

    def consume(self, handler: Handler, wait: bool = False) -> None:
        """Drain due messages; handlers may publish more, which is drained too.

        With `wait=True`, sleeps until delayed messages come due rather than
        returning early — that is what makes the retry path observable in tests.
        """
        while True:
            message = self._pop_due()
            if message is not None:
                handler(message)
                continue

            with self._lock:
                if not self._heap:
                    return
                next_due = self._heap[0][0]
            if not wait:
                return
            time.sleep(max(0.0, next_due - time.monotonic()))

    # --- introspection (tests, demos) ---------------------------------------

    def pending(self) -> int:
        with self._lock:
            return len(self._heap)

    def due(self) -> int:
        now = time.monotonic()
        with self._lock:
            return sum(1 for available_at, _, _ in self._heap if available_at <= now)

    def messages(self) -> list[TaskMessage]:
        with self._lock:
            return [m for _, _, m in sorted(self._heap)]

    def dead_letters(self) -> list[TaskMessage]:
        with self._lock:
            return list(self._dead)

    def close(self) -> None:
        pass


class RabbitMQBroker:
    """Durable queue with manual acknowledgement.

    Connections are created lazily and are not shared across threads — pika
    channels are not thread-safe.
    """

    def __init__(
        self,
        url: str = config.BROKER_URL,
        queue: str = config.TASK_QUEUE,
        dead_letter_queue: str | None = None,
    ):
        self.url = url
        self.queue = queue
        self.dead_letter_queue = dead_letter_queue or config.DEAD_LETTER_QUEUE
        self._connection = None
        self._channel = None
        self._retry_queues: set[str] = set()

    def _ensure_channel(self):
        import pika

        if (
            self._channel is not None
            and self._channel.is_open
            and self._connection is not None
            and self._connection.is_open
        ):
            return self._channel

        self._connection = pika.BlockingConnection(pika.URLParameters(self.url))
        self._channel = self._connection.channel()
        # Durable queues + persistent messages: work survives a broker restart.
        self._channel.queue_declare(queue=self.queue, durable=True)
        self._channel.queue_declare(queue=self.dead_letter_queue, durable=True)
        self._channel.basic_qos(prefetch_count=config.WORKER_PREFETCH)
        self._retry_queues.clear()
        return self._channel

    def _retry_queue(self, delay: float) -> str:
        """A holding queue whose messages expire back onto the main queue.

        Rather than one queue per exact delay, delays are bucketed to the next
        power of two seconds. Every message in a bucket carries the same TTL, so
        they expire in the order they arrived — a single queue with per-message
        TTLs would let a long-delayed message at the head block shorter ones
        behind it.

        No plugins required: `x-message-ttl` expires the message and
        `x-dead-letter-routing-key` routes it back to the main queue.
        """
        seconds = max(1, min(int(2 ** math.ceil(math.log2(max(delay, 1)))),
                             int(math.ceil(config.RETRY_MAX_DELAY))))
        name = f"{self.queue}.retry.{seconds}s"

        if name not in self._retry_queues:
            self._channel.queue_declare(
                queue=name,
                durable=True,
                arguments={
                    "x-message-ttl": int(seconds * 1000),
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": self.queue,
                },
            )
            self._retry_queues.add(name)
        return name

    def _reset(self) -> None:
        """Drop a dead connection so the next call rebuilds it."""
        try:
            if self._connection is not None and self._connection.is_open:
                self._connection.close()
        except Exception:
            pass
        self._connection = None
        self._channel = None
        self._retry_queues.clear()

    def _publish_raw(self, routing_key_for, body: bytes) -> None:
        """Publish, rebuilding the connection once if the stream has gone away.

        A publisher that sits idle — the API between triggers — will eventually
        have its connection reclaimed by the broker or a firewall. The failure
        surfaces on the *next* publish, so one transparent reconnect is the
        difference between a working trigger and a 500.
        """
        import pika

        last_error: Exception | None = None
        for attempt in (1, 2):
            try:
                channel = self._ensure_channel()
                channel.basic_publish(
                    exchange="",
                    routing_key=routing_key_for(),
                    body=body,
                    properties=pika.BasicProperties(delivery_mode=2),  # persistent
                )
                return
            except (pika.exceptions.AMQPError, OSError) as exc:
                last_error = exc
                logger.warning(
                    "publish failed (attempt %s/2): %s; reconnecting", attempt, exc
                )
                self._reset()
        raise last_error

    def publish(self, message: TaskMessage, delay: float = 0.0) -> None:
        # Evaluated after the channel exists, since a retry queue must be declared
        # on the live channel.
        def routing_key():
            return self.queue if delay <= 0 else self._retry_queue(delay)

        self._publish_raw(routing_key, message.to_json())
        logger.debug("published %s (delay %.3fs)", message.task_name, delay)

    def dead_letter(self, message: TaskMessage) -> None:
        """Park a task that exhausted its retries for human inspection."""
        self._publish_raw(lambda: self.dead_letter_queue, message.to_json())
        logger.warning("dead-lettered %s: %s", message.task_name, message.reason)

    def consume(self, handler: Handler) -> None:
        """Block, dispatching deliveries to `handler` until interrupted."""
        channel = self._ensure_channel()

        def on_message(ch, method, properties, body):
            message = TaskMessage.from_json(body)
            try:
                handler(message)
            except Exception:
                logger.exception("handler raised for %s; requeueing", message.task_name)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                return
            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_consume(queue=self.queue, on_message_callback=on_message)
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()

    def purge(self) -> None:
        channel = self._ensure_channel()
        for queue in (self.queue, self.dead_letter_queue, *self._retry_queues):
            channel.queue_purge(queue)

    def close(self) -> None:
        if self._connection is not None and self._connection.is_open:
            self._connection.close()
        self._connection = None
        self._channel = None


_override: Broker | None = None
_memory_broker: InMemoryBroker | None = None
_local = threading.local()


def get_broker() -> Broker:
    """The broker for the calling thread, chosen by BROKER_URL.

    RabbitMQ connections are **per thread**: pika channels are not thread-safe,
    and a worker runs at least two threads (the consumer and the heartbeat /
    recovery sweep). Sharing one connection between them corrupts the stream —
    it surfaces as `Unexpected frame` and a torn-down connection.

    The in-memory broker is deliberately shared: being a single queue across
    threads is the whole point of it.
    """
    if _override is not None:
        return _override

    if config.BROKER_URL.startswith("memory:"):
        global _memory_broker
        if _memory_broker is None:
            _memory_broker = InMemoryBroker()
        return _memory_broker

    broker = getattr(_local, "broker", None)
    if broker is None:
        broker = RabbitMQBroker()
        _local.broker = broker
        logger.debug("created broker for thread %s", threading.current_thread().name)
    return broker


def set_broker(broker: Broker | None) -> None:
    """Override the broker for every thread (tests, demos)."""
    global _override, _memory_broker
    _override = broker
    if broker is None:
        _memory_broker = None
        if hasattr(_local, "broker"):
            del _local.broker
