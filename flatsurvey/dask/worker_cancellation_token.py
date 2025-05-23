from contextlib import contextmanager
import dask.distributed
from collections import defaultdict
import threading


NORMAL = 0
CANCEL = 1
ABORT = 2


class WorkerCancellationToken:
    _abort_callbacks = {}
    _state = {}
    _lock = threading.Lock()

    @staticmethod
    def cancel(token: str):
        WorkerCancellationToken._state[token] = CANCEL

    @staticmethod
    def abort(token: str):
        WorkerCancellationToken._state[token] = ABORT

        with WorkerCancellationToken._lock:
            callbacks = WorkerCancellationToken._abort_callbacks.pop(token, [])

        for callback in callbacks:
            callback()

    def __init__(self, id: str):
        self._id = id

    def is_cancelled(self, client: dask.distributed.Client) -> bool:
        if WorkerCancellationToken._state.get(self._id, NORMAL) >= CANCEL:
            return True

        from typing import cast
        metadata = cast(int, client.get_metadata(self._id, NORMAL))
        return metadata >= CANCEL

    def is_aborted(self, client: dask.distributed.Client) -> bool:
        if WorkerCancellationToken._state.get(self._id, NORMAL) >= ABORT:
            return True

        from typing import cast
        metadata = cast(int, client.get_metadata(self._id, NORMAL))
        return metadata >= ABORT

    @contextmanager
    def on_abort(self, callback):
        with WorkerCancellationToken._lock:
            abort_callbacks = WorkerCancellationToken._abort_callbacks
            abort_callbacks.setdefault(self._id, set())
            abort_callbacks[self._id].add(callback)

        try:
            if self.is_aborted(dask.distributed.get_client()):
                # Note that there is a tiny race here since the callback could
                # already be running before we manage to replace it. Since the
                # callback is assumed to be thread safe and idempotent, it
                # should not matter even if it happens.
                with WorkerCancellationToken._lock:
                    abort_callbacks[self._id].remove(callback)

                callback()
                return

            yield
        finally:
            with WorkerCancellationToken._lock:
                callbacks = abort_callbacks[self._id]
                try:
                    callbacks.remove(callback)
                except KeyError:
                    pass
