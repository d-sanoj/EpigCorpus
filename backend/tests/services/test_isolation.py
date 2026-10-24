"""Work run in a child process: results, errors, and a child that dies."""

from __future__ import annotations

import os
import signal
from functools import partial

import pytest

from epigcorpus.services.isolation import ChildProcessDied, IsolatedWorkFailed, run_isolated


def add(a: int, b: int) -> int:
    return a + b


def fail() -> None:
    raise OSError("disk full")


def killed() -> None:
    # What the kernel's out-of-memory killer does.
    os.kill(os.getpid(), signal.SIGKILL)


def test_returns_the_childs_result():
    assert run_isolated(partial(add, 2, 3)) == 5


def test_an_error_in_the_child_is_raised_in_the_parent():
    with pytest.raises(IsolatedWorkFailed, match="^OSError: disk full$"):
        run_isolated(fail)


def test_a_killed_child_is_reported_as_likely_out_of_memory():
    with pytest.raises(ChildProcessDied, match="killed by signal 9.*ran out of memory") as died:
        run_isolated(killed)
    assert died.value.exitcode == -9
