"""Run memory-heavy work in a child process, so the server keeps its memory.

Cleaning the full corpus allocates gigabytes that pandas does not hand back to
the operating system. Run in the server process, that memory stays with the
server, and if the machine runs short the kernel kills the whole server. In a
child process the memory goes when the child exits, and running short kills
only the child: the server stays up and reports why the stage failed.
"""

from __future__ import annotations

import multiprocessing
import traceback
from collections.abc import Callable
from typing import Any

# A fresh interpreter, not a fork: forking a server with threads and a loaded
# corpus copies state that is not safe to copy.
_CONTEXT = multiprocessing.get_context("spawn")


class ChildProcessDied(RuntimeError):
    """The child ended without a result: killed, most often for lack of memory."""

    def __init__(self, exitcode: int | None):
        self.exitcode = exitcode
        if exitcode is not None and exitcode < 0:
            how = f"was killed by signal {-exitcode}"
        else:
            how = f"exited with code {exitcode}"
        hint = (
            " This usually means the machine (or Docker) ran out of memory;"
            " cleaning the full corpus needs about 4 GB on top of the server."
            if exitcode in (-9, 137)
            else ""
        )
        super().__init__(f"The worker process {how} before finishing.{hint}")


class IsolatedWorkFailed(RuntimeError):
    """The work raised in the child; the message carries its traceback."""


def _child(target: Callable[[], Any], sender) -> None:
    try:
        sender.send(("ok", target()))
    except BaseException as exc:  # noqa: BLE001 - sent to the parent, never lost
        sender.send(("error", f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"))
    finally:
        sender.close()


def run_isolated(target: Callable[[], Any]) -> Any:
    """Call `target()` in a child process and return its result.

    `target` must be picklable: a module-level function, or a
    functools.partial of one.
    """
    receiver, sender = _CONTEXT.Pipe(duplex=False)
    process = _CONTEXT.Process(target=_child, args=(target, sender), daemon=True)
    process.start()
    sender.close()
    try:
        status, payload = receiver.recv()
    except EOFError:
        process.join()
        raise ChildProcessDied(process.exitcode) from None
    finally:
        receiver.close()
    process.join()
    if status == "error":
        raise IsolatedWorkFailed(payload.splitlines()[0]) from RuntimeError(payload)
    return payload
