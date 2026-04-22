"""
utils/worker_pool.py
--------------------
Parallel map that works in ALL environments including Airflow daemons.

The problem
-----------
Airflow 3 runs each task as a daemon process.
Python forbids daemon processes from spawning child processes:
  AssertionError: daemonic processes are not allowed to have children

The fix
-------
Spawn the multiprocessing.Pool from a non-daemon *thread* instead.
A non-daemon thread can freely create child processes even when it lives
inside a daemon process.  The thread runs synchronously (we join it
before returning), so all caller logic is unchanged.

    process_file()  — must be a top-level picklable function (it already is)

Usage
-----
    from utils.worker_pool import parallel_imap_unordered

    for result in parallel_imap_unordered(process_file, file_args,
                                          n_workers=8, chunksize=50):
        handle(result)
"""

import logging
import multiprocessing
import multiprocessing.pool
import queue
import threading
from multiprocessing.pool import ThreadPool
from typing import Any, Callable, Iterable, Iterator

logger = logging.getLogger(__name__)


def parallel_imap_unordered(
    fn: Callable,
    args: Iterable,
    n_workers: int = 8,
    chunksize: int = 50,
) -> Iterator[Any]:
    """
    Run fn over args in parallel, yielding results as they complete.

    Automatically handles the Airflow daemon restriction:
      - Tries true multiprocessing.Pool first (real parallelism, no GIL)
      - Falls back to ThreadPool if process spawning is blocked

    Parameters
    ----------
    fn        : top-level picklable callable (e.g. process_file)
    args      : iterable of arguments, one per call
    n_workers : number of parallel workers
    chunksize : imap_unordered chunksize

    Yields
    ------
    Results in completion order (same as pool.imap_unordered)
    """
    args_list = list(args)   # must be a list — consumed once

    # ── Try true multiprocessing via a non-daemon thread ─────────────────
    # A non-daemon THREAD can always spawn child PROCESSES, even when the
    # parent process itself is a daemon (Airflow task).
    result_queue: queue.Queue = queue.Queue()
    error_queue:  queue.Queue = queue.Queue()

    def _run_pool():
        try:
            with multiprocessing.pool.Pool(n_workers) as pool:
                for r in pool.imap_unordered(fn, args_list, chunksize=chunksize):
                    result_queue.put(r)
        except Exception as exc:
            error_queue.put(exc)
        finally:
            result_queue.put(_SENTINEL)   # signal done

    _SENTINEL = object()

    t = threading.Thread(target=_run_pool, daemon=False)
    t.start()

    # Collect from the queue until we get the sentinel
    while True:
        item = result_queue.get()
        if item is _SENTINEL:
            break
        yield item

    t.join()

    # If pool raised, fall back to ThreadPool
    if not error_queue.empty():
        exc = error_queue.get()
        logger.warning(
            "ProcessPool failed (%s) — falling back to ThreadPool(workers=%d)",
            exc, n_workers,
        )
        with ThreadPool(n_workers) as pool:
            for r in pool.imap_unordered(fn, args_list, chunksize=chunksize):
                yield r
    else:
        logger.debug("ProcessPool completed successfully")
