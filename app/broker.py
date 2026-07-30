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

import json
import logging
import threading
from collections import deque
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

    def to_json(self) -> bytes:
        return json.dumps(
            {
                "task_id": self.task_id,
                "run_id": self.run_id,
                "task_name": self.task_name,
                "attempt": self.attempt,
            }
        ).encode()

    @classmethod
    def from_json(cls, raw: bytes) -> "TaskMessage":
        data = json.loads(raw)
        return cls(
            task_id=data["task_id"],
            run_id=data["run_id"],
            task_name=data["task_name"],
            attempt=data.get("attempt", 0),
        )


# The worker hands back True to ack, False to nack. Phase 3 uses the nack path
# for retries; Phase 2 always acks, since a failed task is a recorded outcome,
# not a delivery failure.
Handler = Callable[[TaskMessage], bool]


class Broker(Protocol):
    def publish(self, message: TaskMessage) -> None: ...

    def consume(self, handler: Handler) -> None: ...

    def close(self) -> None: ...


class InMemoryBroker:
    """In-process queue. Not durable, not cross-process — tests and demos only."""

    def __init__(self) -> None:
        self._queue: deque[TaskMessage] = deque()
        self._lock = threading.Lock()

    def publish(self, message: TaskMessage) -> None:
        with self._lock:
            self._queue.append(message)
        logger.debug("published %s", message.task_name)

    def consume(self, handler: Handler) -> None:
        """Drain the queue. Handlers may publish more work, which is drained too."""
        while True:
            with self._lock:
                if not self._queue:
                    return
                message = self._queue.popleft()
            handler(message)

    def pending(self) -> int:
        with self._lock:
            return len(self._queue)

    def close(self) -> None:
        pass


class RabbitMQBroker:
    """Durable queue with manual acknowledgement.

    Connections are created lazily and are not shared across threads — pika
    channels are not thread-safe.
    """

    def __init__(self, url: str = config.BROKER_URL, queue: str = config.TASK_QUEUE):
        self.url = url
        self.queue = queue
        self._connection = None
        self._channel = None

    def _ensure_channel(self):
        import pika

        if self._channel is not None and self._channel.is_open:
            return self._channel

        self._connection = pika.BlockingConnection(pika.URLParameters(self.url))
        self._channel = self._connection.channel()
        # Durable queue + persistent messages: work survives a broker restart.
        self._channel.queue_declare(queue=self.queue, durable=True)
        self._channel.basic_qos(prefetch_count=config.WORKER_PREFETCH)
        return self._channel

    def publish(self, message: TaskMessage) -> None:
        import pika

        channel = self._ensure_channel()
        channel.basic_publish(
            exchange="",
            routing_key=self.queue,
            body=message.to_json(),
            properties=pika.BasicProperties(delivery_mode=2),  # persistent
        )
        logger.debug("published %s to %s", message.task_name, self.queue)

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
        self._ensure_channel().queue_purge(self.queue)

    def close(self) -> None:
        if self._connection is not None and self._connection.is_open:
            self._connection.close()
        self._connection = None
        self._channel = None


_broker: Broker | None = None


def get_broker() -> Broker:
    """Process-wide broker, chosen by BROKER_URL."""
    global _broker
    if _broker is None:
        _broker = (
            InMemoryBroker()
            if config.BROKER_URL.startswith("memory:")
            else RabbitMQBroker()
        )
    return _broker


def set_broker(broker: Broker | None) -> None:
    """Override the process-wide broker (tests)."""
    global _broker
    _broker = broker
