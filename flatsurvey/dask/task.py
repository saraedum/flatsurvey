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

    >>> client = Client(processes=False, nthreads=1, preload="flatsurvey.worker.dask")

    >>> from flatsurvey.surfaces import Ngon, Surface
    >>> from flatsurvey.jobs import OrbitClosure
    >>> from flatsurvey.pipeline import Pipeline

    >>> survey = Pipeline()
    >>> survey.append("goals", OrbitClosure)

    >>> pipeline1 = survey.clone()
    >>> pipeline1.define(Surface, Ngon(angles=[1, 1, 1], length="e-antic"))

    >>> pipeline2 = survey.clone()
    >>> pipeline2.define(Surface, Ngon(angles=[1, 1, 2], length="e-antic"))

    >>> tasks = [DaskTask(pipeline=pipeline1), DaskTask(pipeline=pipeline2)]
    >>> tasks
    [DaskTask(…), DaskTask(…)]

    >>> futures = [client.submit(task) for task in tasks]
    >>> results = [future.result() for future in futures]

Note that the futures have no actual result, the result is usually written to
some log file by a reporter instead::

    >>> results
    [None, None]

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

class Task:
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

        >>> from flatsurvey.surfaces import Ngon, Surface
        >>> from flatsurvey.jobs import OrbitClosure
        >>> from flatsurvey.pipeline import Pipeline

        >>> pipeline = Pipeline()
        >>> pipeline.define(Surface, Ngon(angles=[1, 1, 1], length="e-antic"))
        >>> pipeline.append("goals", OrbitClosure)

        >>> task = DaskTask(pipeline=pipeline)
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

    # TODO: We should probably force that the parameter here is "pipeline" since that's the only thing the worker understands. Unless we allow configuration of the Worker.
    def __init__(self, pipeline, token: SchedulerCancellationToken, repr="DaskTask(…)"):
        from pickle import dumps

        self._repr = repr
        self._pipeline = dumps(pipeline)
        self._token = token.id

    def __call__(self):
        r"""
        Execute this task in the current worker and return the result.

        This is the callable that is submitted via ``client.submit`` by the scheduler.

        TODO: Add an example that shows how exceptions are handled.

        EXAMPLES::

            >>> from flatsurvey.surfaces import Ngon, Surface
            >>> from flatsurvey.jobs import OrbitClosure
            >>> from flatsurvey.pipeline import Pipeline

            >>> pipeline = Pipeline()
            >>> pipeline.define(Surface, Ngon(angles=[1, 1, 1], length="e-antic"))
            >>> pipeline.append("goals", OrbitClosure)

            >>> task = DaskTask(pipeline=pipeline)
            >>> task()

        """
        return DaskRunner(self, DaskRunnerCancellationToken(self._token)).run()

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

            >>> from flatsurvey.surfaces import Ngon, Surface
            >>> from flatsurvey.jobs import OrbitClosure
            >>> from flatsurvey.pipeline import Pipeline

            >>> pipeline = Pipeline()
            >>> pipeline.define(Surface, Ngon(angles=[1, 1, 1], length="e-antic"))
            >>> pipeline.append("goals", OrbitClosure)

            >>> task = DaskTask(pipeline=pipeline)
            >>> task.run()
            [Ngon([1, 1, 1])] [OrbitClosure] dimension: 2/2
            [Ngon([1, 1, 1])] [OrbitClosure] GL(2,R)-orbit closure of dimension at least 2 in H_1(0) (ambient dimension 2) (dimension: 2) (directions: 1) (directions_with_cylinders: 1) (dense: True)

        """
        from pickle import loads

        try:
            pipeline = loads(self._pipeline)
        except Exception as e:
            import pickletools
            raise ValueError(f"Failed to unpickle job: {pickletools.dis(self._pipeline)}") from e
            
        import asyncio

        from flatsurvey.worker import Worker

        return asyncio.run(Worker.work(pipeline, limits=DaskTask.LIMITS))
