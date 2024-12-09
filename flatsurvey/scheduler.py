r"""
Runs a survey with dask on the local machine or in cluster.

TODO: Give full examples.
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


class Scheduler:
    r"""
    A scheduler that splits a survey into commands that are sent out to workers
    via the dask protocol.

    >>> Scheduler(generators=[], bindings=[], goals=[], reporters=[])
    Scheduler

    """

    def __init__(
        self,
        generators,
        bindings,
        goals,
        reporters,
        scheduler=None,
        queue=16,
        dry_run=False,
        quiet=False,
        debug=False,
    ):
        self._generators = generators
        self._bindings = bindings
        self._goals = goals
        self._reporters = reporters
        self._scheduler = scheduler
        self._queue_limit = queue
        self._dry_run = dry_run
        self._quiet = quiet
        self._debug = debug

        self._enable_shared_bindings()

    def __repr__(self):
        return "Scheduler"

    async def _create_pool(self):
        r"""
        Return a new dask pool to schedule jobs.
        """
        import dask.config

        dask.config.set({"distributed.worker.daemon": False})

        import dask.distributed

        return await dask.distributed.Client(
            scheduler_file=self._scheduler,
            direct_to_workers=True,
            connection_limit=2**16,
            asynchronous=True,
            n_workers=8,
            nthreads=1,
            preload="flatsurvey.worker.dask",
            # Disable the dask nanny, see module documentation of worker/dask.py
            processes=False,
        )

    async def start(self):
        r"""
        Run the scheduler until it has run out of jobs to schedule.

        >>> import asyncio
        >>> scheduler = Scheduler(generators=[], bindings=[], goals=[], reporters=[])
        >>> asyncio.run(scheduler.start())

        """
        pool = await self._create_pool()

        try:
            try:
                from flatsurvey.ui.progress import SurveyProgress
                with SurveyProgress(activity="running survey") as progress:
                    from more_itertools import roundrobin

                    surfaces = roundrobin(*self._generators)

                    pending = []

                    async def schedule_one():
                        scheduled = await self._schedule(
                            pool,
                            pending,
                            surfaces,
                            self._goals,
                            progress
                        )
                        return scheduled

                    async def consume_one():
                        completed = await self._consume(pool, pending)
                        if completed:
                            progress.completed()
                        return completed

                    # Fill the job queue with a base line of queue_limit many jobs.
                    for _ in range(self._queue_limit):
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
            await pool.close(0)

    def _enable_shared_bindings(self):
        shared = [binding for binding in self._bindings if binding.scope == "SHARED"]

        import pinject

        import flatsurvey.reporting.report

        objects = pinject.new_object_graph(
            modules=[flatsurvey.reporting.report], binding_specs=shared
        )

        def share(binding):
            if binding.scope == "SHARED":
                from flatsurvey.pipeline.util import provide

                object = provide(binding.name, objects)

                from flatsurvey.pipeline.util import FactoryBindingSpec

                return FactoryBindingSpec(lambda: object, binding.name)

            return binding

        self._bindings = [share(binding) for binding in self._bindings]

    async def _schedule(self, pool, pending, surfaces, goals, progress):
        while True:
            surface = next(surfaces, None)

            if surface is None:
                return False

            if await self._resolve_goals_from_cache(surface, self._goals):
                # Everything could be answered from cached data. Proceed to next surface.
                continue

            bindings = list(self._bindings)
            bindings = [
                binding for binding in bindings if not hasattr(binding, "provide_cache")
            ]
            bindings.append(SurfaceBindingSpec(surface))

            from flatsurvey.worker.dask import DaskTask

            task = DaskTask(
                repr=f"DaskTask(surface={surface!r}, goals=[{(", ".join(goal.__name__) for goal in goals)}])",
                bindings=bindings, goals=self._goals, reporters=self._reporters
            )

            progress.queued()
            pending.append(pool.submit(task))
            return True

    async def _consume(self, pool, pending):
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
                await job
            except Exception as e:
                logging.error(f"Task crashed with {e}. Skipping.")

        return True

    async def _resolve_goals_from_cache(self, surface, goals):
        r"""
        Return whether all ``goals`` could be resolved from cached data.
        """
        bindings = list(self._bindings)

        from flatsurvey.pipeline.util import FactoryBindingSpec, ListBindingSpec

        bindings.append(FactoryBindingSpec(lambda: surface, "surface"))
        bindings.append(ListBindingSpec("goals", goals))
        bindings.append(ListBindingSpec("reporters", self._reporters))

        from random import randint

        bindings.append(FactoryBindingSpec(lambda: randint(0, 2**64), "lot"))

        import pinject

        import flatsurvey.cache
        import flatsurvey.jobs
        import flatsurvey.reporting
        import flatsurvey.surfaces

        objects = pinject.new_object_graph(
            modules=[
                flatsurvey.reporting,
                flatsurvey.surfaces,
                flatsurvey.jobs,
                flatsurvey.cache,
            ],
            binding_specs=bindings,
        )

        class Goals:
            def __init__(self, goals):
                self._goals = goals

        goals = [goal for goal in objects.provide(Goals)._goals]

        for goal in goals:
            await goal.consume_cache()

        goals = [goal for goal in goals if goal._resolved != goal.COMPLETED]

        return not goals


import pinject


class SurfaceBindingSpec(pinject.BindingSpec):
    def __init__(self, surface):
        self._surface = surface

    def provide_surface(self):
        return self._surface
