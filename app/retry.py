"""Retry policy: how long to wait before re-queueing a failed task.

Exponential backoff with jitter. The delay doubles per attempt so a persistently
broken dependency is not hammered, and the jitter keeps a batch of tasks that
failed together from retrying in lockstep and re-colliding.
"""

from __future__ import annotations

import random

from app import config


def backoff_delay(
    attempt: int,
    *,
    base: float | None = None,
    maximum: float | None = None,
    jitter: float | None = None,
    rng: random.Random | None = None,
) -> float:
    """Seconds to wait before retry number `attempt` (1 = first retry).

    >>> backoff_delay(1, base=1, maximum=60, jitter=0)
    1.0
    >>> backoff_delay(4, base=1, maximum=60, jitter=0)
    8.0
    """
    base = config.RETRY_BASE_DELAY if base is None else base
    maximum = config.RETRY_MAX_DELAY if maximum is None else maximum
    jitter = config.RETRY_JITTER if jitter is None else jitter

    if attempt < 1:
        raise ValueError("attempt is 1-based")

    delay = min(base * (2 ** (attempt - 1)), maximum)
    if jitter:
        spread = delay * jitter
        delay += (rng or random).uniform(-spread, spread)
    return max(0.0, delay)


def should_retry(retry_count: int, max_retries: int) -> bool:
    """`max_retries` is the number of retries *after* the first attempt."""
    return retry_count < max_retries
