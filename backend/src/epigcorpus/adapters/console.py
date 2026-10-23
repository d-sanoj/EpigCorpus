"""The scraper's stdout writer: one lock, so worker threads never interleave lines."""

import threading

_print_lock = threading.Lock()


def log(message):
    with _print_lock:
        print(message, flush=True)
