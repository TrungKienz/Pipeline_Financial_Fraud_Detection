from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import timedelta

from .models import TransactionEvent


def clone_event(event: TransactionEvent, sequence: int, step_offset: int = 0, time_offset_seconds: int = 0) -> TransactionEvent:
    new_event_id = hashlib.sha1(f"{event.event_id}:{sequence}:{step_offset}:{time_offset_seconds}".encode("utf-8")).hexdigest()
    return replace(
        event,
        event_id=new_event_id,
        step=event.step + step_offset,
        event_time=event.event_time + timedelta(seconds=time_offset_seconds),
        producer_ts=event.producer_ts + timedelta(seconds=time_offset_seconds),
        name_orig=f"{event.name_orig}_{sequence % 1000}",
        name_dest=f"{event.name_dest}_{sequence % 1000}",
    )


def synthesize_events(seed_events: list[TransactionEvent], target_count: int) -> list[TransactionEvent]:
    if not seed_events:
        return []
    generated: list[TransactionEvent] = []
    for index in range(target_count):
        template = seed_events[index % len(seed_events)]
        generated.append(
            clone_event(
                template,
                sequence=index,
                step_offset=index // max(1, len(seed_events)),
                time_offset_seconds=index,
            )
        )
    return generated
