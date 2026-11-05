"""KCensus-style adaptive offered-load search, independent of execution."""
from dataclasses import dataclass
import math
import random


@dataclass(frozen=True)
class SweepSettings:
    rates: list
    repetitions: int = 3
    latency_cap_ms: float = 400.
    order_seed: int = 20261103

    def validate(self):
        if not isinstance(self.rates, list) or not self.rates or any(type(r) is not int or r < 1 for r in self.rates):
            raise ValueError('sweep rates must be positive integers')
        if self.rates != sorted(set(self.rates)):
            raise ValueError('sweep rates must be distinct and increasing')
        if type(self.repetitions) is not int or self.repetitions < 1 or type(self.order_seed) is not int:
            raise ValueError('positive repetition count and integer order seed required')
        if not math.isfinite(self.latency_cap_ms) or self.latency_cap_ms <= 0:
            raise ValueError('positive finite latency cap required')


def sweep_schedule(settings):
    """Independent repetitions; never stop or select a retry based on latency."""
    settings.validate()
    rng = random.Random(settings.order_seed)
    for repetition in range(1, settings.repetitions+1):
        rates = list(settings.rates)
        rng.shuffle(rates)
        for rate in rates:
            yield rate, repetition


@dataclass(frozen=True)
class Settings:
    start: int = 500
    ceiling: int = 1024000
    coarse_step: int = 2000
    step_floor: int = 500
    latency_cap_ms: float = 400.
    knee_factor: float = 1.2
    attempts: int = 2

    def validate(self):
        for key in ('start', 'ceiling', 'coarse_step', 'step_floor', 'attempts'):
            if type(getattr(self, key)) is not int or getattr(self, key) < 1:
                raise ValueError('positive integer required: '+key)
        if self.start > self.ceiling or self.step_floor > self.coarse_step:
            raise ValueError('invalid search bounds')
        if not math.isfinite(self.latency_cap_ms) or self.latency_cap_ms <= 0:
            raise ValueError('positive finite latency cap required')
        if not math.isfinite(self.knee_factor) or self.knee_factor <= 1:
            raise ValueError('knee factor must exceed one')


def adaptive(settings, evaluate):
    """evaluate(rate, phase) returns a validated point or raises on invalid evidence.

    Each point exposes sustained, mean_ms, and achieved_rps. Retries and caching
    belong to evaluate so the search never mistakes infrastructure failure for
    a capacity boundary. The result is a finite-load/latency bracket, not proof
    of unbounded steady-state capacity.
    """
    settings.validate()
    seen = {}

    def point(rate, phase):
        if rate not in seen:
            seen[rate] = evaluate(rate, phase)
        return seen[rate]

    rate, sustained = settings.start, None
    while rate <= settings.ceiling:
        row = point(rate, 'doubling')
        if not row['sustained']:
            break
        sustained = rate
        rate *= 2
    if sustained is None:
        return dict(outcome='below_start', highest_sustained_offered_rps=None,
                    first_unsustained_offered_rps=settings.start, final_step=None)
    if rate > settings.ceiling:
        return dict(outcome='ceiling_reached', highest_sustained_offered_rps=sustained,
                    first_unsustained_offered_rps=None, final_step=None)

    step = max(1, sustained//2)
    while True:
        rate = sustained + step
        if rate > settings.ceiling:
            break
        row = point(rate, 'refinement')
        if row['sustained']:
            sustained = rate
        reduce = step > settings.coarse_step or (
            step > sustained//8 and step > settings.step_floor)
        if reduce and step > 1:
            step = max(1, step//2)
        elif not row['sustained']:
            break

    # Fill three lower points, then walk downward to <=1.2x the low-load mean.
    for below in (3, 2, 1):
        rate = sustained - below*step
        if rate >= settings.start:
            point(rate, 'near_knee')
    position, back = sustained-3*step, step
    baseline = point(settings.start, 'baseline')['mean_ms']
    while position >= settings.start:
        row = point(position, 'knee_backfill')
        if row['mean_ms'] is not None and row['mean_ms'] < settings.knee_factor*baseline:
            break
        back *= 2
        while 2*back > position and back > settings.start:
            back = max(1, back//2)
        position = ((position-1)//back)*back

    # Later backfill can expose non-monotonic behavior. Preserve that evidence.
    best = max(rate for rate, row in seen.items() if row['sustained'])
    above = [rate for rate, row in seen.items() if rate > best and not row['sustained']]
    return dict(outcome='bracketed' if above else 'unbracketed_nonmonotonic',
                highest_sustained_offered_rps=best,
                first_unsustained_offered_rps=min(above) if above else None,
                final_step=step, low_load_mean_ms=baseline)
