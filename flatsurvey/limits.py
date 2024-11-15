import logging

logger = logging.getLogger()

class Limit:
    def __init__(self, limit):
        self._limit = limit

    def check(self):
        raise NotImplementedError("this Limit does not implement check() yet")


class LimitChecker:
    def __init__(self, limit, callback, period=30):
        self._limit = limit
        self._callback = callback
        self._period = period

        self._task = None

    def start(self):
        assert self._task is None

        import asyncio
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._run())

    def stop(self):
        if self._task is not None:
            self._task.cancel()
            self._task = None

    def check(self):
        if not self._limit.check():
            self._callback()
            self.stop()
    
    async def _run(self):
        try:
            while True:
                self.check()

                import asyncio
                await asyncio.sleep(self._period)
        except Exception as e:
            import traceback
            logging.error(f"limit checker crashed with {''.join(traceback.format_exception(e))}")


class TimeLimit(Limit):
    def __init__(self, limit):
        super().__init__(limit)

        self._start = None

    @staticmethod
    def parse_limit(limit):
        import pandas
        return pandas.Timedelta(limit).to_pytimedelta() 

    def check(self):
        import time

        now = time.time()

        if self._start is None:
            self._start = now

        verdict = now - self._start <= self._limit.total_seconds()

        if not verdict:
            logging.warning("time limit exceeded")

        return verdict


class MemoryLimit(Limit):
    @staticmethod
    def parse_limit(limit):
        import psutil
        ram = psutil.virtual_memory().total
        cpus = psutil.cpu_count()

        if limit == "conservative":
            return int(ram / cpus / 2)

        if limit == "standard":
            return int(ram / cpus)

        if limit == "overcommit":
            return int(ram / cpus * 1.5)

        from datasize import DataSize
        return int(DataSize(limit))

    @staticmethod
    def memory():
        import os
        pid = os.getpid()

        smap = f"/proc/{pid}/smaps"

        entries = {}

        with open(smap, "r") as smap:
            for line in smap:
                items = line.split()

                if not items[0].endswith(':'):
                    continue
                if len(items) != 3:
                    continue
                if items[2] != 'kB':
                    continue

                key = items[0][:-1]
                value = int(items[1]) * 1024
                entries[key] = entries.get(key, 0) + value

        return entries

    def check(self):
        smap = MemoryLimit.memory()
        verdict = smap["Rss"] + smap["Swap"] <= self._limit

        if not verdict:
            logging.warning("memory limit exceeded")

        return verdict
