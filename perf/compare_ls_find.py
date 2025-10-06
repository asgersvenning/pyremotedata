import os
import random
import tempfile
import time
from argparse import ArgumentParser
from math import floor, log10

from tqdm.auto import trange

from pyremotedata.config import ask_user
from pyremotedata.implicit_mount import IOHandler

units = {
    86400 : "day",
    3600 : "hour",
    60 : "minute",
    1 : "second",
    1e-3 : "millisecond",
    1e-6 : "microsecond",
    1e-9 : "nanosecond"
}

abr = {
    "day" : "d",
    "hour" : "h",
    "minute" : "m",
    "second" : "s",
    "millisecond" : "ms",
    "microsecond" : "μs",
    "nanosecond" : "ns"
}

class Duration:
    @staticmethod
    def _normalize(m: int | float, e: int) -> tuple[int | float, int]:
        if m == 0:
            return 0, 0
        if isinstance(m, int):
            while m % 10 == 0:
                m //= 10
                e += 1
            return m, e
        am = abs(m)
        k = floor(log10(am))
        m = m / (10 ** k)
        e = e + k
        return m, e

    def __init__(self, value : int | float, exp : int=0):
        if value < 0:
            raise ValueError("Duration cannot be negative.")
        
        self.orig_value = value
        self.orig_exp = exp
        
        log_value = log10(self.orig_value if self.orig_value != 0 else 1) + self.orig_exp
        for v, unit in sorted(units.items(), reverse=True):
            self.unit, self.exp = unit, log10(v)
            if log_value > self.exp:
                break
        self.value = self.orig_value * 10**(self.orig_exp - self.exp)

    def __add__(self, other: "Duration") -> "Duration":
        if isinstance(other, (int, float)) and other == 0:
            return Duration(self.orig_value, self.orig_exp)
        if not isinstance(other, Duration):
            return NotImplemented
        a, ea = self.orig_value, self.orig_exp
        b, eb = other.orig_value, other.orig_exp
        if a == 0:
            return Duration(b, eb)
        if b == 0:
            return Duration(a, ea)
        e = min(ea, eb)
        A = a * (10 ** (ea - e))
        B = b * (10 ** (eb - e))
        m = A + B
        m, e = Duration._normalize(m, e)
        return Duration(m, e)

    def __radd__(self, other: object) -> "Duration":
        if isinstance(other, (int, float)) and other == 0:
            return Duration(self.orig_value, self.orig_exp)
        if isinstance(other, Duration):
            return other.__add__(self)
        return NotImplemented

    def __iadd__(self, other: "Duration") -> "Duration":
        return self.__add__(other)
    
    def __mul__(self, other: int | float) -> "Duration":
        if not isinstance(other, (int, float)):
            return NotImplemented
        if other < 0:
            raise ValueError("Multiplication only allowed with non-negative scalars.")
        if self.orig_value == 0 or other == 0:
            return Duration(0)
        m = self.orig_value * other
        e = self.orig_exp
        m, e = Duration._normalize(m, e)
        return Duration(m, e)

    __rmul__ = __mul__

    def __truediv__(self, other: int | float) -> "Duration":
        if not isinstance(other, (int, float)):
            return NotImplemented
        if other <= 0:
            raise ValueError("Division only allowed with positive scalars.")
        if self.orig_value == 0:
            return Duration(0)
        m = self.orig_value / other
        e = self.orig_exp
        m, e = Duration._normalize(m, e)
        return Duration(m, e)
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Duration):
            return NotImplemented
        return self.exp == other.exp and self.value == other.value

    def __lt__(self, other: "Duration") -> bool:
        if not isinstance(other, Duration):
            return NotImplemented
        if self.exp != other.exp:
            return self.exp < other.exp
        return self.value < other.value
    
    def __bool__(self):
        return True

    @property
    def _int_part(self):
        return floor(self.value)
    
    @property
    def _remainder(self):
        return self.value - self._int_part
    
    def int_format(self):
        return f'{self._int_part}{abr[self.unit]}'

    def __repr__(self):
        return str(self)
    
    def __str__(self):
        parts = [self.int_format()]
        rem = Duration(self._remainder, self.exp)
        while len(parts) < 3 and rem.unit != self.unit and rem.value > 1e-12:
            parts.append(rem.int_format())
            rem = Duration(rem._remainder, rem.exp)
        return " ".join(parts)

def duration_quantile(ds: list[Duration], q: float) -> Duration:
    if not 0.0 <= q <= 1.0:
        raise ValueError("q must be in [0,1].")
    if not ds:
        raise ValueError("Empty sequence.")
    ys = sorted(ds)  # uses Duration.__lt__
    n = len(ys)
    if n == 1:
        return ys[0]
    pos = (n - 1) * q
    lo = int(pos)
    hi = min(lo + 1, n - 1)
    if lo == hi:
        return ys[lo]
    frac = pos - lo
    return ys[lo] * (1 - frac) + ys[hi] * frac

class Timer:
    def __init__(self):
        self.start = None
        self.end = None

    @property
    def duration(self):
        if self.start is None:
            return None
        ref = self.end if self.end is not None else time.perf_counter_ns()
        return Duration(ref - self.start, exp=-9)

    @property
    def status(self):
        if self.end is not None:
            return "Done"
        if self.start is not None:
            return "Running"
        return "Waiting"

    def __enter__(self, *args, **kwargs):
        self.start = time.perf_counter_ns()
        self.end = None
        return self

    def __exit__(self, *args, **kwargs):
        self.end = time.perf_counter_ns()

    def __repr__(self):
        return f'Timer[{self.status}]: {self.duration or "N.d."}'


def time_ls(io : IOHandler, verbose : bool=False):
    timer = Timer()
    with timer:
        result = io.ls(recursive=True, use_cache=True)
    if verbose:
        print(f"`ls`:\n  * {timer} to find {len(result)} files.\n")
    return timer.duration

def time_find(io : IOHandler, verbose : bool=False):
    timer = Timer()
    with timer:
        result = io.execute_command("find -d 9999 | cat")
    if verbose:
        print(f"`find -d 9999`:\n  * {timer} to find {len(result)} files.\n")
    return timer.duration

def time_rm(io : IOHandler, verbose : bool=False):
    timer = Timer()
    with timer:
        cwd = io.pwd()
        io.cd("..")
        io.rm(cwd, force=True)
    if verbose:
        print(f"`rm`:\n  * {timer} to remove {cwd}")
    return timer.duration

if __name__ == "__main__":
    parser = ArgumentParser("compare_ls_find")
    parser.add_argument("-d", "--directory", type=str, required=True)

    args = parser.parse_args()
    directory : str = args.directory

    timings : dict[str, list[Duration]] = {
        "ls" : [],
        "find" : [],
        "rm" : []
    }

    with tempfile.TemporaryDirectory() as td:
        for _ in trange(1000, desc="Creating random files..."):
            with tempfile.NamedTemporaryFile(dir=td, delete=False) as f:
                f.write(os.urandom(10_000))
        for i in trange(10, desc="Timing LFTP..."):
            with IOHandler() as io:
                io.cd("/")
                io.upload(td, directory)
                io.cd(directory)
                io.execute_command("cache flush")
                if random.uniform(0, 1) > 0.5:
                    ls_time = time_ls(io)
                    find_time = time_find(io)
                else:
                    find_time = time_find(io)
                    ls_time = time_ls(io)
                rm_time = time_rm(io)
                if i > 3:
                    timings["ls"].append(ls_time)
                    timings["find"].append(find_time)
                    timings["rm"].append(rm_time)
            time.sleep(0.025)

    for method, ts in timings.items():
        total = sum(ts) / len(ts)
        print(f'"{method}" average:\n * {total} [{duration_quantile(ts, 0.25)} - {duration_quantile(ts, 0.75)}]\n')
