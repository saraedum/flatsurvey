r"""
Implementation of a job that runs on a dask client.

When our main driver communicates with the dask scheduler, there are some
limitations what kind of objects we can safely and efficiently put on the queue
of jobs to process. A :class:`DaskTask` encodes such a job which is executed by
the :class:`DaskRunner`.

We load this module into the actual dask workers with ``--preload
flatsurvey.worker.dask``.

EXAMPLES:

This is a support module for the :mod:`flatsurvey.scheduler` it is not meant to
be used in isolation. Nevertheless, for the sake of testing, we can spin up a
dask client and have it run a task from this module::

    >>> from dask.distributed import Client

    >>> client = Client(processes=False, preload=["flatsurvey.worker.dask"])

    >>> from flatsurvey.surfaces import Ngon
    >>> from flatsurvey.jobs.orbit_closure import OrbitClosure
    >>> from flatsurvey.pipeline.util import PartialBindingSpec
    
    >>> surfaces = [PartialBindingSpec(Ngon, name="surface")(angles=[1, 1, n], length="e-antic") for n in [1, 2]]
    >>> tasks = [DaskTask(goals=[OrbitClosure], bindings=[surface]) for surface in surfaces]
    >>> tasks
    [DaskTask(…), DaskTask(…)]

    >>> futures = [client.submit(task) for task in tasks]
    >>> results = [future.result() for future in futures]

    >>> client.shutdown()

.. NOTE::

    A lot is complicated here by SageMath (and its dependencies) being very
    picky about how things are set up when running in forked processes.

    If we import sage.all in a worker, we get an exception from cysignals:
    "signal only works in main thread of the main interpreter". The dask worker
    runs workloads in a separate thread but SageMath does not like to get
    imported on a non-main thread.

    We could just preload sage.all into the dask worker (``--preload``.)
    However, SageMath still does not like to do things in a threaded
    environment and typically cypari2 segfaults in such a setup.

    To work around this, we fork from the worker thread. Since importing
    sage.all is very costly, we use a forkserver and ``set_forkserver_preload``
    sage.all once. We use one thread per core in the dask worker which then
    translates to one forked process per core. (We further instruct SageMath to
    work in a non-parallel mode via ``MKL_NUM_THREADS=1 SAGE_NUM_THREADS=1
    OMP_NUM_THREADS=1``.)

    We cannot use a dask nanny with our workers since the nanny spawns the
    worker as a daemon, and a daemon canot easily have children in Python (see
    https://mail.python.org/pipermail/python-list/2011-March/600152.html.) So
    we must start our workers with ``--no-nanny``.

    Finally, this approach also works around memory leaks. SageMath tends to
    leak memory over time. Starting the worker from scratch with every single
    computation has a bit of a performance impact but usually there is not that
    much overlap between computations anyway.

"""
# *********************************************************************
#  This file is part of flatsurvey.
#
#        Copyright (C) 2024 Julian Rüth
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
import multiprocessing

import click

# We import sage.all before forking off child processes.
# Importing sage.all is very costly, so we import it once in the template
# process that gets then cloned to produce the workers in the fork calls.
forkserver = multiprocessing.get_context("forkserver")
multiprocessing.set_forkserver_preload(["sage.all"])


class DaskTask:
    r"""
    A task to execute on a dask worker.

    Note that the inputs are not stored directly in the ``__dict__`` of this
    object. Instead, we store a serialization of these objects. This is crucial
    to make SageMath and dask cooperate. The first thing that a dask worker
    does is to fork, so there is a nanny process and an actual worker doing the
    work. However, SageMath does not like to be in a process that was forked in
    that way. Therefore, we shouldn't unpickle these objects because that
    triggers the loading of SageMath. Also, even without the nanny, these
    objects tend to cause memory leaks (due to UniqueRepresentation for
    example.) Therefore, we only unpickle objects in a fresh process that then
    does the actual processing, so we start with a clean SageMath session every
    time.

    INPUT:

    The arguments are passed on to :class:`worker.Worker.work`. All arguments
    must be serializable.

    EXAMPLES:

    We define a task to be executed on the worker::

        >>> from flatsurvey.surfaces import Ngon
        >>> from flatsurvey.jobs.orbit_closure import OrbitClosure
        >>> from flatsurvey.pipeline.util import PartialBindingSpec

        >>> surface = PartialBindingSpec(Ngon, name="surface")(angles=[1, 1, 1], length="e-antic")
        >>> task = DaskTask(goals=[OrbitClosure], bindings=[surface])
        >>> task
        DaskTask(…)

    Normally, the task is going to be serialized, sent to a worker,
    deserialized, and then gets called on the worker::

        >>> task()

    Calling a task like this makes sure that the necessary machinery is set up
    on the worker, i.e., a :class:`DaskRunner` gets created which forks off
    the actual execution process, sets up message queues, …; eventually, the
    task's :meth:`run` is executed and the result of that call is reported
    back (in our setup, only an exception is reported; otherwise the returned
    value is ``None``.)

    """

    # Globally enforced runtime limits that apply to all tasks, see dask_setup
    # below.
    LIMITS = []

    def __init__(self, *args, repr="DaskTask(…)", **kwargs):
        from pickle import dumps

        self._repr = repr
        self._dump = dumps((args, kwargs))

    def __call__(self):
        r"""
        Execute this task in the current worker and return the result.

        EXAMPLES::

            >>> from flatsurvey.surfaces import Ngon
            >>> from flatsurvey.jobs.orbit_closure import OrbitClosure
            >>> from flatsurvey.pipeline.util import PartialBindingSpec

            >>> surface = PartialBindingSpec(Ngon, name="surface")(angles=[1, 1, 1], length="e-antic")
            >>> task = DaskTask(goals=[OrbitClosure], bindings=[surface])
            >>> task()

        """
        return DaskRunner(self).run()

    def __repr__(self):
        r"""
        Return a printable representation for this task.

        We bake the description of this task in during construction since we do
        not want to unpickle any of the ``_dump`` to determine the repr of this
        object when printing status messages.

        >>> DaskTask(repr="DaskTask(1337)")
        DaskTask(1337)

        """
        return self._repr

    def run(self):
        r"""
        Execute the workload specified by this task in the current process and
        return the result.

        This method is meant to be invoked in a remote worker by
        :class:`TaskRunner`.

        EXAMPLES::

            >>> from flatsurvey.surfaces import Ngon
            >>> from flatsurvey.jobs.orbit_closure import OrbitClosure
            >>> from flatsurvey.pipeline.util import PartialBindingSpec

            >>> surface = PartialBindingSpec(Ngon, name="surface")(angles=[1, 1, 1], length="e-antic")
            >>> task = DaskTask(goals=[OrbitClosure], bindings=[surface])
            >>> task.run()
            [Ngon([1, 1, 1])] [OrbitClosure] dimension: 2/2
            [Ngon([1, 1, 1])] [OrbitClosure] GL(2,R)-orbit closure of dimension at least 2 in H_1(0) (ambient dimension 2) (dimension: 2) (directions: 1) (directions_with_cylinders: 1) (dense: True)

        """
        from pickle import loads

        args, kwargs = loads(self._dump)

        assert "limits" not in kwargs, "limits is a reserved keyword that can only be set by the worker"

        kwargs["limits"] = DaskTask.LIMITS

        import asyncio

        from flatsurvey.worker.worker import Worker

        return asyncio.run(Worker.work(*args, **kwargs))


class DaskRunner:
    r"""
    Executes a :class:`DaskTask` in this worker.

    This works around limitations that arise when combining dask and SageMath,
    see module documentation.

    Instances of this are created by :meth:`DaskTask.__call__`. There should be
    no use case to instantiate this otherwise.
    """
    def __init__(self, task: DaskTask):
        self._task = task

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
        process = forkserver.Process(target=DaskRunner._run, args=(self,), daemon=False, name=repr(self._task))
        process.start()
        try:
            self._result_sender.close()
            self._shutdown_receiver.close()

            # Block until the worker is done with the computation.
            _ = self._result_receiver.recv()
            self._result_receiver.close()

            result = self._result_queue.get()

            self._shutdown_sender.send("SHUTDOWN")
            self._shutdown_sender.close()

            return result
        finally:
            process.kill()

    @staticmethod
    def _run(self):  # pyright: ignore
        r"""
        Run a :class:`DaskTask`.

        This method is meant to run in a separate clean process that
        :meth:`run` spawns.
        """
        self._shutdown_sender.close()
        self._result_receiver.close()

        from threading import Thread
        Thread(target=DaskRunner._wait_for_shutdown, args=(self,))

        try:
            try:
                result = self._task.run()
            except Exception as e:
                result = e

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


@click.command()
@click.option(
    # We cannot call this --memory-limit because dask-worker uses this already.
    "--mem-limit",
    default=None,
    help="Gracefully stop a task when the memory consumption exceeds this amount",
)
@click.option(
    "--time-limit",
    default=None,
    help="Gracefully stop a task when the wall time elapsed exceeds this amount",
)
def dask_setup(_, mem_limit, time_limit):
    r"""
    A parser that we inject into the dask worker command line parser.

    Dask executes this ``dask_setup`` with command line arguments that it
    cannot make sense of since we preload this module into the dask worker.

    We use this to set global runtime limits that are stored in the global
    ``limits`` variable in this module.
    """
    global limits
    if mem_limit is not None:
        from flatsurvey.limits import MemoryLimit

        DaskTask.LIMITS.append(MemoryLimit(MemoryLimit.parse_limit(mem_limit)))

    if time_limit is not None:
        from flatsurvey.limits import TimeLimit

        DaskTask.LIMITS.append(TimeLimit(TimeLimit.parse_limit(time_limit)))
