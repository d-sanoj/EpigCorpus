"""Scraper command line: ``python -m app.cli.scrape [--limit N] [--workers N] ...``.

Parses arguments, installs the SIGTERM handler, and wires the real adapters
into the harvest use case.
"""

import argparse
import signal

from app.adapters import edcs_client, harvest_store
from app.adapters.console import log
from app.config.settings import DEFAULT_WORKERS
from app.services.scrape import run_harvest


def install_signal_handlers():
    """Make SIGTERM stop the harvest as cleanly as Ctrl+C does.

    Background jobs started from a non-interactive shell inherit SIGINT set to
    ignore, so `kill -INT` does nothing and the only way to stop a detached run
    is SIGTERM. Without this, SIGTERM kills the process mid-chunk and leaves the
    output files ahead of the checkpoint, which produces duplicate rows on the
    next resume. Raising KeyboardInterrupt routes SIGTERM into the same handler
    that already flushes and checkpoints.
    """

    def handler(signum, _frame):
        raise KeyboardInterrupt(f"signal {signum}")

    signal.signal(signal.SIGTERM, handler)


def build_parser():
    parser = argparse.ArgumentParser(description="Harvest EDCS inscriptions.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only harvest the first N monuments (smoke test).")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Concurrent requests (default {DEFAULT_WORKERS}).")
    parser.add_argument("--restart", action="store_true",
                        help="Discard existing output and checkpoint, then start over.")
    parser.add_argument("--refresh-index", action="store_true",
                        help="Re-fetch the corpus index instead of using the cache.")
    return parser


def main():
    args = build_parser().parse_args()
    install_signal_handlers()
    run_harvest(
        args.limit,
        args.workers,
        args.restart,
        args.refresh_index,
        client=edcs_client,
        store=harvest_store,
        log=log,
    )


if __name__ == "__main__":
    main()
