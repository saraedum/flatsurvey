import click

import multiprocessing
forkserver = multiprocessing.get_context("forkserver")
multiprocessing.set_forkserver_preload(["sage.all"])


limits = []


class DaskTask:
    def __init__(self, *args, **kwargs):
        from pickle import dumps
        self._dump = dumps((args, kwargs))

    def __call__(self):
        DaskWorker.process(self)

    # TODO: Add the string repr of the surface so we can produce a repr without unpickling anything expensive.

    def run(self):
        from pickle import loads
        args, kwargs =loads(self._dump)

        assert "limits" not in kwargs

        kwargs["limits"] = limits

        from flatsurvey.worker.worker import Worker

        import asyncio
        result = asyncio.run(Worker.work(*args, **kwargs))
        return result


class DaskWorker:
    _singleton = None

    def __init__(self):
        assert DaskWorker._singleton is None

        self._work_queue = forkserver.Queue()
        self._result_queue = forkserver.Queue()
        self._processor = forkserver.Process(target=DaskWorker._process, args=(self,), daemon=True)
        self._processor.start()

    @staticmethod
    def _ensure_started():
        if DaskWorker._singleton is not None:
            return

        import sys
        if 'sage' in sys.modules:
            raise Exception("sage must not be loaded in dask worker")

        DaskWorker._singleton = DaskWorker()

    @staticmethod
    def _process(self):
        while True:
            try:
                task = self._work_queue.get()
            except ValueError:
                break

            self._result_queue.put(task.run())

    @staticmethod
    def process(task):
        DaskWorker._ensure_started()
        DaskWorker._singleton._work_queue.put(task)
        return DaskWorker._singleton._result_queue.get()


@click.command()
@click.option(
    # We cannot call this --memory-limit because dask-worker uses this already.
    "--mem-limit",
    default=None,
    help="Gracefully stop a task when the memory consumption exceeds this amount")
@click.option(
    "--time-limit",
    default=None,
    help="Gracefully stop a task when the wall time elapsed exceeds this amount")
def dask_setup(mem_limit, time_limit):
    global limits
    if mem_limit is not None:
        from flasturvey.limits import MemoryLimit
        limits.append(MemoryLimit(MemoryLimit.parse_limit(mem_limit)))

    if time_limit is not None:
        from flatsurvey.limits import TimeLimit
        limits.append(TimeLimit(TimeLimit.parse_limit(time_limit)))
