"""Opt-in deterministic context for isolated schedule validation.

Normal FieldStation42 callers do not activate this context and retain the
existing clock, RNG, ordering, and error-handling behavior.
"""

import asyncio
import contextvars
import datetime
import random
import threading
from contextlib import contextmanager
from dataclasses import dataclass


VALIDATION_TIMEZONE = "America/Los_Angeles"


@dataclass(frozen=True)
class ValidationSchedulingContext:
    reference_clock: datetime.datetime
    start_time: datetime.datetime
    end_time: datetime.datetime
    seed: int
    timezone: str = VALIDATION_TIMEZONE
    validation_mode: bool = True

    def __post_init__(self):
        for name in ("reference_clock", "start_time", "end_time"):
            value = getattr(self, name)
            if not isinstance(value, datetime.datetime):
                raise TypeError(f"{name} must be a datetime")
            if value.tzinfo is not None:
                raise ValueError(f"{name} must use FieldStation42 naive local time")
        if self.start_time >= self.end_time:
            raise ValueError("validation schedule range must have start before end")
        if isinstance(self.seed, bool) or not isinstance(self.seed, int):
            raise TypeError("validation seed must be an integer")
        if self.timezone != VALIDATION_TIMEZONE:
            raise ValueError(f"validation timezone must be {VALIDATION_TIMEZONE}")
        if self.validation_mode is not True:
            raise ValueError("ValidationSchedulingContext requires validation_mode=true")


class _Activation:
    __slots__ = ("context", "active", "owner_thread", "owner_task", "_rng")

    def __init__(self, context):
        self.context = context
        self.active = True
        self.owner_thread = threading.get_ident()
        self.owner_task = _current_task()
        self._rng = random.Random(context.seed)

    def verify(self):
        if not self.active:
            raise RuntimeError("validation scheduling activation has expired")
        if threading.get_ident() != self.owner_thread or _current_task() is not self.owner_task:
            raise RuntimeError("validation scheduling activation crossed an execution boundary")

    def invoke_rng(self, name, args, kwargs):
        self.verify()
        return getattr(self._rng, name)(*args, **kwargs)


class _RNGProxy:
    __slots__ = ("__activation",)

    def __init__(self, activation):
        object.__setattr__(self, "_RNGProxy__activation", activation)

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)

        def guarded(*args, **kwargs):
            activation = object.__getattribute__(self, "_RNGProxy__activation")
            return activation.invoke_rng(name, args, kwargs)

        return guarded

    def __setattr__(self, name, value):
        raise AttributeError("validation RNG proxy is immutable")


def _current_task():
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


_ACTIVE_CONTEXT = contextvars.ContextVar(
    "fs42_validation_scheduling_activation", default=None
)


def current_validation_context():
    activation = _ACTIVE_CONTEXT.get()
    if activation is None:
        return None
    activation.verify()
    return activation.context


def in_validation_mode():
    context = current_validation_context()
    return context is not None and context.validation_mode is True


def scheduling_now():
    context = current_validation_context()
    if context is not None:
        return context.reference_clock
    return datetime.datetime.now()


def scheduling_random():
    activation = _ACTIVE_CONTEXT.get()
    if activation is not None:
        activation.verify()
        return _RNGProxy(activation)
    return random


def validation_order(values, *, key=None):
    if in_validation_mode():
        return sorted(values, key=key)
    return values


@contextmanager
def activate_validation_context(context):
    if not isinstance(context, ValidationSchedulingContext):
        raise TypeError("an explicit ValidationSchedulingContext is required")
    if _ACTIVE_CONTEXT.get() is not None:
        raise RuntimeError("nested validation scheduling activation is not allowed")
    activation = _Activation(context)
    token = _ACTIVE_CONTEXT.set(activation)
    try:
        yield context
    finally:
        activation.active = False
        _ACTIVE_CONTEXT.reset(token)
