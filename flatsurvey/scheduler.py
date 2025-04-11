r"""
Runs a survey with dask on the local machine or in cluster.

This implemnts a (somewhat unnecessary) wrapper for the dask API.

EXAMPLES:

We compute the orbit closure of the (1,1,1) and the (1,1,2) triangles::
    
    >>> from flatsurvey.surfaces import Ngons
    >>> ngons = Ngons.click.callback(3, 'e-antic', min=0, limit=None, count=2, literature='include', family=None, filter=None)

    >>> from flatsurvey.jobs import OrbitClosure

    >>> scheduler = Scheduler(generators=[ngons], goals=[OrbitClosure], bindings=[], reporters=[])

    >>> import asyncio
    >>> asyncio.run(scheduler.start())  # random progress output
    on ...: all jobs have been scheduled
    done ...

"""
# *********************************************************************
#  This file is part of flatsurvey.
#
#        Copyright (C) 2020-2024 Julian Rüth
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

import logging

from typing import Iterator

from flatsurvey.pipeline import Pipeline
from flatsurvey.surfaces import Surface


class Scheduler:
    r"""
    A scheduler that splits a survey into commands that are sent out to workers
    via the dask protocol.

    INPUT::

    - ``generators`` -- a list of generators of surfaces, e.g., a list of lists
      of surfaces

    - ``goals`` -- a list of goals that should be resolved for each surface
      such as :class:`OrbitClosure`.

    - ``reporters`` -- a list of reporters that should be used to report the
      ``goals`` (default: the empty list which means the reporting will be done
      to stdout)

    - ``bindings`` -- additional bindings that are not covered by
      ``generators``, ``goals``, and ``reporters`` that should be taken into
      account by the dependency injection (default: the empty list.)

    - ``scheduler`` -- a dask scheduler file to connect to; if not given (the
      default) then a dask scheduler is started by this process

    - ``queue`` -- the number of processes to initially submit into the dask
      scheduler before we wait for workers to finish (default: thrice the
      number of workers threads)

    - ``debug`` -- whether to attach a debbugger when the scheduler crashes
      (default: ``False``.)

    EXAMPLES::

    >>> Scheduler(generators=[], goals=[])
    Scheduler(…)

    """

    def __init__(
        self,
        survey_pipeline: Pipeline,
        scheduler_json=None,
        queue_limit=None,
        debug=False,
    ):
        self._survey_pipeline = survey_pipeline
        self._scheduler_json = scheduler_json
        self._queue_limit = queue_limit
        self._debug = debug

    def __repr__(self):
        return "Scheduler(…)"

    async def _create_pool(self):
        r"""
        Return a new dask pool to schedule jobs.
        """
        import dask.config

        # We do not spawn workers as daemons so that they can have child
        # processes, see worker/dask.py (only has an effect if no
        # scheduler_file is set.)
        dask.config.set({"distributed.worker.daemon": False})

        import dask.distributed

        from multiprocessing import cpu_count

        return await dask.distributed.Client(
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

    async def start(self):
        r"""
        Run the scheduler until all jobs have been scheduled and all jobs
        terminated.

        >>> import asyncio
        >>> scheduler = Scheduler(generators=[], bindings=[], goals=[], reporters=[])
        >>> asyncio.run(scheduler.start())  # random progress output
        on ...: all jobs have been scheduled
        done ...

        """
        pool = await self._create_pool()

        try:
            try:
                from flatsurvey.ui.progress import SurveyProgress
                with SurveyProgress(activity="running survey") as progress:
                    from more_itertools import roundrobin
                    surfaces = roundrobin(*self._survey_pipeline.get("surfaces"))

                    pending = []

                    async def schedule_one():
                        scheduled = await self._schedule_one(
                            pool=pool,
                            pending=pending,
                            surfaces=surfaces,
                            progress=progress,
                        )
                        return scheduled

                    async def consume_one():
                        completed = await self._consume(pending=pending)
                        if completed:
                            progress.completed()
                        return completed

                    # Fill the job queue with a base line of queue_limit many jobs.
                    for _ in range(self._queue_limit or 3 * sum((await pool.nthreads()).values())):
                        if not await schedule_one():
                            break

                    try:
                        # Wait for a result. For each result, schedule a new task.
                        while await consume_one():
                            if not await schedule_one():
                                break
                    except KeyboardInterrupt:
                        # TODO: This does not work. The exception is not thrown here.
                        print("stopped scheduling of new jobs as requested")
                    else:
                        print("all jobs have been scheduled")
                    progress.set_activity("waiting for pending tasks")

                    try:
                        # Wait for all pending tasks to finish.
                        while await consume_one():
                            pass
                    except KeyboardInterrupt:
                        print("not awaiting schedule jobs anymore as requested")
                        raise

                    progress.set_activity("done")
            except Exception:
                if self._debug:
                    import pdb

                    pdb.post_mortem()

                raise
        finally:
            # Terminate all workers immediately if we crash out of this code
            # block. (If we terminated normally, then there's nothing we have
            # to wait for.
            await pool.close(0) # pyright: ignore[reportGeneralTypeIssues]

    async def _schedule_one(self, *, pool, pending, surfaces: Iterator[Surface], progress):
        r"""
        Enqueue another surface for computation on a worker.

        Return whether there was another surface, i.e., ``False`` iff all surfaces have been scheduled already.

        This will skip over surfaces that can be resolved from cached data.

        This is a helper method for :meth:`start`.
        """
        while True:
            surface = next(surfaces, None)

            if surfaces is None:
                return False

            pipeline = self._survey_pipeline.clone()
            pipeline.forget("surfaces")
            pipeline.define(Surface, surface)

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

            progress.queued()
            pending.append(pool.submit(task))
            return True

    async def _consume(self, pending):
        import dask.distributed

        completed, still_pending = await dask.distributed.wait(
            pending, return_when="FIRST_COMPLETED"
        )

        pending.clear()
        pending.extend(still_pending)

        if not completed:
            return False

        for job in completed:
            try:
                result = await job
                assert not isinstance(result, Exception)
            except Exception as e:
                logging.error(f"Task crashed with {e}. Skipping.")

        return True

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
