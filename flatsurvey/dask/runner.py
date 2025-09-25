# *********************************************************************
#  This file is part of flatsurvey.
#
#        Copyright (C) 2024-2025 Julian Rüth
#
#  flatsurvey is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  flatsurvey is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with flatsurvey. If not, see <https://www.gnu.org/licenses/>.
# *********************************************************************

from flatsurvey.dask.worker import forkserver

from flatsurvey.dask.task import Task
from flatsurvey.dask.worker_cancellation_token import WorkerCancellationToken


class Runner:
    r"""
    Executes a :class:`Task` in this worker.

    This works around limitations that arise when combining dask and SageMath,
    see module documentation.

    Instances of this are created by :meth:`Task.__call__`. There should be
    no use case to instantiate this otherwise.
    """
    def __init__(self, task: Task, token: WorkerCancellationToken):
        self._task = task
        self._token = token

        # We use a pipe to signal that the computation is complete. The
        # advantage over a queue is that if the worker process crashes, we
        # receive an EOF when trying to read from it.
        self._result_receiver, self._result_sender = forkserver.Pipe(duplex=False)

        # We use a pipe to signal to the worker process that we are still
        # waiting for the computation. A thread in the worker process tries to
        # read from the pipe and crashes when the other side closes it.
        self._shutdown_receiver, self._shutdown_sender = forkserver.Pipe(duplex=False)

        # Since a pipe is limited to 32MB, we use an actual queue to
        # communicate the result back. (Then again, we usually do not send any
        # non-trivial data back but store survey results on a shared disk
        # directly from the worker.)
        self._result_queue = forkserver.Queue()

    def run(self):
        # We do not set daemon, so that the process can have child processes.
        # The pipe setup makes sure that the child dies nevertheless when the
        # parent dies.
        # For most workloads this does not seem to be necessary, and we might
        # want to change that at some point.
        # TODO: What happens when _run raises an Exception? Add a test.
        process = forkserver.Process(target=Runner._run, args=(self,), daemon=False, name=repr(self._task))

        import dask.distributed
        if self._token.is_cancelled(dask.distributed.get_client()):
            return

        import threading
        kill_lock = threading.Lock()

        def kill():
            # Since process.kill is not thread safe, we wrap it with a simple
            # lock. (Just in case that on_abort and finally kick in at the same
            # time.)
            with kill_lock:
                process.kill()

        process.start()
        try:
            with self._token.on_abort(kill):
                self._result_sender.close()
                self._shutdown_receiver.close()

                # Block until the worker is done with the computation.
                _ = self._result_receiver.recv()
                self._result_receiver.close()

                result = self._result_queue.get()

                self._shutdown_sender.send("SHUTDOWN")
                self._shutdown_sender.close()

                if isinstance(result, Exception):
                    raise result
                return result
        finally:
            kill()

    @staticmethod
    def _run(self):  # pyright: ignore
        r"""
        Run a :class:`Task`.

        This method is meant to run in a separate clean process that
        :meth:`run` spawns.
        """
        self._shutdown_sender.close()
        self._result_receiver.close()

        from threading import Thread
        Thread(target=Runner._wait_for_shutdown, args=(self,))

        try:
            try:
                result = self._task.run()
            except Exception:
                import traceback
                result = RunnerException(f"exception occurred in runner\n{traceback.format_exc()}")

            self._result_sender.send("DONE")

            self._result_queue.put(result)
        finally:
            self._result_queue.close()
            self._result_sender.close()

    @staticmethod
    def _wait_for_shutdown(self):  # pyright: ignore
        r"""
        Wait for a shutdown signal from the parent process.

        This method is meant to run in a separate thread that kills this
        process when the parent disappears or signals us to terminate.
        """
        try:
            _ = self._shutdown_receiver.recv()
            self._shutdown_receiver.close()
        except:
            import sys
            sys.exit()


class RunnerException(Exception):
    pass


