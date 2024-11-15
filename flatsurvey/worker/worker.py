r"""
Entrypoint for the survey worker to solve a single work package.

Invoke this providing a source and some goals, e.g., to compute the orbit closure of a quadrilateral:
```
python -m survey.worker ngon -a 1 -a 2 -a 3 -a 4 orbit-closure
```

TESTS::

    >>> from flatsurvey.test.cli import invoke
    >>> invoke(worker) # doctest: +NORMALIZE_WHITESPACE
    Usage: worker [OPTIONS] COMMAND1 [ARGS]... [COMMAND2 [ARGS]...]...
      Explore a surface.
    Options:
      --debug
      --help             Show this message and exit.
      --mem-limit TEXT   Gracefully stop the worker when the memory consumption
                         exceeds this amount
      --time-limit TEXT  Gracefully stop the worker when the wall time elapsed
                         exceeds this amount
      -v, --verbose      Enable verbose message, repeat for debug message.
    Cache:
      local-cache  A cache of previous results stored in local JSON files.
      pickles      Access a database of pickles storing parts of previous
                   computations.
    Goals:
      completely-cylinder-periodic  Determines whether for all directions given by
                                    saddle connections, the decomposition of the
                                    surface is completely cylinder periodic, i.e.,
                                    the decomposition consists only of cylinders.
      cylinder-periodic-direction   Determines whether there is a direction for
                                    which the surface decomposes into cylinders.
      orbit-closure                 Determines the GL₂(R) orbit closure of
                                    ``surface``.
      undetermined-iet              Tracks undetermined Interval Exchange
                                    Transformations.
    Intermediates:
      flow-decompositions             Turns directions coming from saddle
                                      connections into flow decompositions.
      saddle-connection-orientations  Orientations of saddle connections on the
                                      surface, i.e., the vectors of saddle
                                      connections irrespective of scaling and sign.
      saddle-connections              Saddle connections on the surface.
    Reports:
      json      Writes results in JSON format.
      log       Writes progress and results as an unstructured log file.
      progress  Reports progress on the command line.
      report    Generic reporting of results.
    Surfaces:
      ngon            Unfolding of an n-gon with prescribed angles.
      pickle          A base64 encoded pickle.
      thurston-veech  Thurston-Veech construction

"""
# *********************************************************************
#  This file is part of flatsurvey.
#
#        Copyright (C) 2020-2022 Julian Rüth
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

import click
import pinject

import flatsurvey.cache
import flatsurvey.jobs
import flatsurvey.reporting
import flatsurvey.surfaces
from flatsurvey.pipeline.util import FactoryBindingSpec, ListBindingSpec
from flatsurvey.ui.group import CommandWithGroups
from flatsurvey.worker.restart import Restart


@click.group(
    chain=True,
    cls=CommandWithGroups,
    help=r"""Explore a surface.""",
)
@click.option("--debug", is_flag=True)
@click.option(
    "--mem-limit",
    default=None,
    help="Gracefully stop the worker when the memory consumption exceeds this amount")
@click.option(
    "--time-limit",
    default=None,
    help="Gracefully stop the worker when the wall time elapsed exceeds this amount")
@click.option(
    "--verbose",
    "-v",
    count=True,
    help="Enable verbose message, repeat for debug message.",
)
def worker(debug, mem_limit, time_limit, verbose):
    r"""
    Main command to invoke the worker; specific objects and goals are
    registered automatically as subcommands.

    """


# Register subcommands
for kind in [
    flatsurvey.surfaces.commands,
    flatsurvey.jobs.commands,
    flatsurvey.reporting.commands,
    flatsurvey.cache.commands,
]:
    for command in kind:
        worker.add_command(command)


@worker.result_callback()
def process(commands, debug, mem_limit, time_limit, verbose):
    r"""
    Run the specified subcommands of ``worker``.

    EXAMPLES:

    We compute the orbit closure of the unfolding of a equilateral triangle,
    i.e., the torus::

        >>> from flatsurvey.test.cli import invoke
        >>> invoke(worker, "ngon", "-a", "1", "-a", "1", "-a", "1", "orbit-closure")
        [Ngon([1, 1, 1])] [OrbitClosure] dimension: 2/2
        [Ngon([1, 1, 1])] [OrbitClosure] GL(2,R)-orbit closure of dimension at least 2 in H_1(0) (ambient dimension 2) (dimension: 2) (directions: 1) (directions_with_cylinders: 1) (dense: True)

    """
    if debug:
        import pdb
        import signal

        signal.signal(signal.SIGUSR1, lambda sig, frame: pdb.Pdb().set_trace(frame))

    if verbose:
        import logging

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG if verbose > 1 else logging.INFO)

    limits = []
    if mem_limit is not None:
        from flatsurvey.limits import MemoryLimit
        limits.append(MemoryLimit(MemoryLimit.parse_limit(mem_limit)))

    if time_limit is not None:
        from flatsurvey.limits import TimeLimit
        limits.append(TimeLimit(TimeLimit.parse_limit(time_limit)))

    try:
        import asyncio
        asyncio.run(Worker.work(commands=commands, limits=limits))
    except Exception:
        if debug:
            pdb.post_mortem()
        raise


class Worker:
    r"""
    Works on a set of ``goals`` until they are all resolved.

    EXAMPLES::

        >>> import asyncio
        >>> worker = Worker(goals=[], reporters=[])
        >>> start = worker.start()
        >>> asyncio.run(start)

    """

    @pinject.copy_args_to_internal_fields
    def __init__(
        self,
        goals,
        reporters,
    ):
        pass

    @classmethod
    async def work(cls, /, bindings=[], goals=[], reporters=[], commands=[], limits=[]):
        objects = Worker.make_object_graph(bindings=bindings, goals=goals, reporters=reporters, commands=commands)

        try:
            await objects.provide(Worker).start(limits=limits)
        except Restart as restart:
            bindings = [restart.rewrite_binding(binding, objects=objects) for binding in bindings]
            goals = [restart.rewrite_goal(goal, objects=objects) for goal in goals]
            reporters = [restart.rewrite_reporter(reporter, objects=objects) for reporter in reporters]
            for command in commands:
                command = restart.rewrite_command(command, objects=objects)

                bindings.extend(command["bindings"])
                goals.extend(command["goals"])
                reporters.extend(command["reporters"])

            await Worker.work(bindings=bindings, goals=goals, reporters=reporters, commands=[], limits=limits)

    @classmethod
    def make_object_graph(cls, /, bindings=[], goals=[], reporters=[], commands=[]):
        bindings = list(bindings)
        goals = list(goals)
        reporters = list(reporters)

        for command in commands:
            bindings.extend(command.get("bindings", []))
            goals.extend(command.get("goals", []))
            reporters.extend(command.get("reporters", []))

        bindings.append(ListBindingSpec("goals", goals))
        bindings.append(
            ListBindingSpec("reporters", reporters or [flatsurvey.reporting.Log])
        )
        from random import randint

        bindings.append(FactoryBindingSpec("lot", lambda: randint(0, 2**64)))

        return pinject.new_object_graph(
            modules=[
                flatsurvey.reporting,
                flatsurvey.surfaces,
                flatsurvey.jobs,
                flatsurvey.cache,
            ],
            binding_specs=bindings,
        )

    async def start(self, limits=[]):
        r"""
        Run until all our goals are resolved.
        """
        def callback():
            for goal in self._goals:
                from flatsurvey.pipeline.goal import Goal
                goal._resolved = Goal.COMPLETED

        from flatsurvey.limits import LimitChecker
        checks = [LimitChecker(limit, callback) for limit in limits]

        for check in checks:
            check.start()

        try:
            try:
                for goal in self._goals:
                    await goal.consume_cache()
                for goal in self._goals:
                    await goal.resolve()
            finally:
                for goal in self._goals:
                    await goal.report()
        finally:
            for check in checks:
                check.stop()

        for reporter in self._reporters:
            reporter.flush()


if __name__ == "__main__":
    worker()
