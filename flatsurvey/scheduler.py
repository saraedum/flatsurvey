r"""
Runs a survey with dask on the local machine or in a cluster.

This implements a (somewhat unnecessary) wrapper for the dask API.

EXAMPLES:

We compute the orbit closure of the (1,1,1) and the (1,1,2) triangles::
    
    >>> from flatsurvey.pipeline.pipeline import Pipeline
    >>> survey = Pipeline()

    >>> from flatsurvey.surfaces import Ngons
    >>> ngons = Ngons(vertices=3, length="e-antic", min=0, limit=None, count=2, literature='include', family=None, filter=None)
    >>> survey.append("surfaces", ngons)

    >>> from flatsurvey.jobs import OrbitClosure
    >>> survey.append("goals", OrbitClosure)

    >>> scheduler = Scheduler(survey_pipeline=survey)

    >>> import asyncio
    >>> asyncio.run(scheduler.start())  # random progress output
    on ...: all jobs have been scheduled
    waiting for jobs to finish ...

"""
# *********************************************************************
#  This file is part of flatsurvey.
#
#        Copyright (C) 2020-2025 Julian Rüth
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

from contextlib import contextmanager

from flatsurvey.pipeline import Pipeline
from flatsurvey.surfaces import Surface
from flatsurvey.ui.progress import SurveyProgress


class Scheduler:
    r"""
    A scheduler that splits a survey into commands that are sent out to workers
    via the dask protocol.

    INPUT::

    - ``survey_pipeline`` -- a :class:`Pipeline` that specifies which
      computations should be performed by this survey. This pipeline must have
      a ``"surfaces"`` entry for all the surfaces that should be surveyed.

    - ``scheduler`` -- a dask scheduler file to connect to; if not given (the
      default) then a dask scheduler is started by this process

    - ``queue_limit`` -- the number of processes to initially submit into the
      dask scheduler before we wait for workers to finish (default: thrice the
      number of workers threads provided by the dask scheduler)

    EXAMPLES::

    >>> from flatsurvey.pipeline.pipeline import Pipeline
    >>> pipeline = Pipeline()
    >>> pipeline.append("surfaces", [])

    >>> Scheduler(survey_pipeline=pipeline)
    Scheduler(…)

    """

    def __init__(
        self,
        survey_pipeline: Pipeline,
        scheduler_json=None,
        queue_limit=None,
    ):
        self._survey_pipeline = survey_pipeline
        self._scheduler_json = scheduler_json
        self._queue_limit = queue_limit

        import dask.distributed
        self._pool: dask.distributed.Client | None = None
        self._cancellation_requested = False
        self._pending_jobs = []

    def __repr__(self):
        return "Scheduler(…)"

    async def _create_pool(self):
        r"""
        Return a new dask pool to schedule jobs.
        """
        assert self._pool is None, "cannot recreate dask pool"

        import dask.config

        # We do not spawn workers as daemons so that they can have child
        # processes, see worker/dask.py (only has an effect if no
        # scheduler_file is set.)
        dask.config.set({"distributed.worker.daemon": False})

        import dask.distributed

        from multiprocessing import cpu_count

        self._pool = await dask.distributed.Client(
            scheduler_file=self._scheduler_json,
            direct_to_workers=True,
            # Connections are not very expensive but this number should be big
            # enough so that we can communicate with all workers in large
            # clusters.
            connection_limit=2**16,
            # We want to use dask through its modern asynchronous API.
            asynchronous=True,
            # Start a worker for each execution thread on the CPU. (Only
            # relevant if we are not using an external scheduler.)
            n_workers=cpu_count(),
            # We run each worker single-threaded, see worker/dask.py.
            nthreads=1,
            # We preload worker/dask.py unless scheduler_file is set, see
            # documentation there.
            preload="flatsurvey.worker.dask",
            # Disable the dask nanny, see module documentation of worker/dask.py
            processes=False,
        )
        self._pending_jobs = []

    @contextmanager
    def _create_sigint_handler(self):
        # The signal handling that SageMath (or rather cysignals) installs for
        # the SIGINT (i.e., Ctrl-C) signal does not play well in the (async?)
        # dask world. What exactly happens is unclear but somehow when our
        # _consume() entered dask.distributed.wait() and then we press Ctrl-C,
        # a SIGABRT is raised (or at least it says "Aborted!" in the terminal)
        # and then this asynchronous execution thread basically disappears. The
        # program eventually exits but not exception is raised, and no finally
        # blocks are executed here.
        # Whatever is going on exactly, we don't want any special handling of
        # Ctrl-C in the scheduling process. When Ctrl-C is pressed while
        # SageMath is doing something (say doing a bit of arithmetic to figure
        # out what's the next surface to survey) that's usually very fast and
        # we cannot interrupt it safely anyway. So we do not rely on the
        # KeyboardInterrupt at all here but just handle SIGINT ourselves to
        # cancel/abort the survey.
        def handle_sigint(_, __):
            if self._cancellation_requested:
                print("forcing scheduler to shut down.")
                if self._pool is None:
                    # The Dask client has not connected yet. We are going to
                    # wait for dask to boot and then stop the computation
                    # immediately.
                    return

                import asyncio
                asyncio.get_running_loop().create_task(self._pool.close(0))  # pyright: ignore[reportArgumentType]
            else:
                print("requested cancellation")
                self._cancellation_requested = True

        import signal
        from cysignals.pysignals import changesignal

        with changesignal(signal.SIGINT, handle_sigint):
            self._cancellation_requested = False
            yield

    def _create_surfaces(self):
        from more_itertools import roundrobin
        self._surfaces = roundrobin(*self._survey_pipeline.get("surfaces"))

    async def start(self):
        r"""
        Run the scheduler until all jobs have been scheduled and all jobs
        terminated.

        >>> import asyncio
        >>> from flatsurvey.pipeline.pipeline import Pipeline
        >>> pipeline = Pipeline()
        >>> pipeline.append("surfaces", [])
        >>> scheduler = Scheduler(survey_pipeline=pipeline)
        >>> asyncio.run(scheduler.start())  # random progress output
        on ...: no jobs were required to complete this survey
        ...

        """
        with self._create_sigint_handler():
            await self._create_pool()

            try:
                self._create_surfaces()

                with SurveyProgress(activity="...") as progress:
                    await self._seed_jobs(progress)

                    if not self._pending_jobs:
                        print("no jobs were required to complete this survey")
                        return

                    await self._submit_jobs(progress)
                    await self._await_pending_jobs(progress)
            finally:
                # Terminate all workers immediately if we crash out of this code
                # block. (If we terminated normally, then there's nothing we have
                # to wait for.
                assert self._pool is not None
                await self._pool.close(0)  # pyright: ignore[reportGeneralTypeIssues]

    async def _seed_jobs(self, progress):
        progress.set_activity("seeding job queue")

        assert self._pool is not None

        # Fill the job queue with a base line of queue_limit many jobs.
        for _ in range(self._queue_limit or 3 * sum((await self._pool.nthreads()).values())):
            if not await self._submit_job(progress):
                return

    async def _submit_jobs(self, progress):
        progress.set_activity("scheduling jobs as needed")

        # Wait for a result. For each result, schedule a new task.
        while True:
            if self._cancellation_requested:
                print("stopped scheduling of new jobs as requested")
                return

            assert self._pending_jobs, "_submit_jobs needs jobs to wait for to keep the job queue filled"

            completed = await self._await_pending_job(progress)
            assert completed, "await_pending_job must only return when a job terminated"

            for _ in range(completed):
                if not await self._submit_job(progress):
                    if self._cancellation_requested:
                        print("stopped scheduling of new jobs as requested")
                        return

                    print("all jobs have been scheduled")
                    return

    async def _await_pending_jobs(self, progress):
        progress.set_activity("waiting for jobs to finish")
        while await self._await_pending_job(progress):
            pass

    async def _submit_job(self, progress):
        r"""
        Enqueue another surface for computation on a worker.

        Return whether there was another surface, i.e., ``False`` iff all surfaces have been scheduled already.

        This will skip over surfaces that can be resolved from cached data.

        This is a helper method for :meth:`start`.
        """
        assert self._pool is not None

        while True:
            if self._cancellation_requested:
                return False

            surface = next(self._surfaces, None)

            if surface is None:
                return False

            pipeline = self._survey_pipeline.clone()
            pipeline.forget("surfaces")
            pipeline.define(Surface, surface)

            if self._cancellation_requested:
                return False

            cached = await self._resolve_from_cache(pipeline)

            assert surface._surface.cache is None, "to prevent memory leaks, surface must not be created to resolve caches"

            if cached:
                # Everything could be answered from cached data. Proceed to next surface.
                continue

            pipeline = pipeline.clone()

            from flatsurvey.worker.dask import DaskTask

            task = DaskTask(
                repr=f"DaskTask(surface={surface!r}, goals={pipeline.describe("goals")})",
                pipeline=pipeline
            )

            if self._cancellation_requested:
                return False

            progress.queued()
            self._pending_jobs.append(self._pool.submit(task))
            return True

    async def _await_pending_job(self, progress: SurveyProgress):
        if not self._pending_jobs:
            return 0

        import dask.distributed

        completed, still_pending = await dask.distributed.wait(
            self._pending_jobs, return_when="FIRST_COMPLETED"
        )

        self._pending_jobs = list(still_pending)

        assert completed, "wait() only returns when a job terminates"

        for job in completed:
            progress.completed()
            try:
                result = await job
                assert not isinstance(result, Exception)
            except Exception as e:
                print(f"Task crashed with {e}. Skipping.")

        return len(completed)

    async def _resolve_from_cache(self, pipeline):
        r"""
        Return whether all ``goals`` could be resolved from cached data.

        This is a helper method for :meth:`_schedule_one`.
        """
        goals = pipeline.get("goals")

        for goal in goals:
            await goal.consume_cache()

        pending_goals = [goal for goal in goals if goal._resolved != goal.COMPLETED]

        return not pending_goals
