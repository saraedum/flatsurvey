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
      json    Writes results in JSON format.
      log     Writes progress and results as an unstructured log file.
      report  Generic reporting of results.
    Surfaces:
      ngon            Unfolding of an n-gon with prescribed angles.
      pickle          A base64 encoded pickle.
      thurston-veech  Thurston-Veech construction

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

import click

import flatsurvey.cache
import flatsurvey.jobs
import flatsurvey.reporting
import flatsurvey.surfaces
from flatsurvey.pipeline import Bindings
from flatsurvey.ui.group import CommandWithGroups
from flatsurvey.reporting.report import Report
from flatsurvey.restart import Restart


@click.group(
    chain=True,
    cls=CommandWithGroups,
    help=r"""Explore a surface.""",
)
@click.option("--debug", is_flag=True)
@click.option(
    "--mem-limit",
    default=None,
    help="Gracefully stop the worker when the memory consumption exceeds this amount",
)
@click.option(
    "--time-limit",
    default=None,
    help="Gracefully stop the worker when the wall time elapsed exceeds this amount",
)
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

    from flatsurvey.pipeline import Bindings

    bindings = Bindings()

    for command in commands:
        command(bindings)

    limits = []
    if mem_limit is not None:
        from flatsurvey.dask.limits import MemoryLimit

        limits.append(MemoryLimit(MemoryLimit.parse_limit(mem_limit)))

    if time_limit is not None:
        from flatsurvey.dask.limits import TimeLimit

        limits.append(TimeLimit(TimeLimit.parse_limit(time_limit)))

    try:
        import asyncio

        asyncio.run(Worker.work(bindings=bindings, limits=limits))
    except Exception:
        if debug:
            pdb.post_mortem()
        raise


class Worker:
    r"""
    Works on a set of ``goals`` until they are all resolved.

    EXAMPLES::

        >>> import asyncio
        >>> from flatsurvey.reporting.report import Report
        >>> worker = Worker(goals=[], report=Report(reporters=[]))
        >>> start = worker.start()
        >>> asyncio.run(start)

    """

    def __init__(
        self,
        goals,
        report: Report,
    ):
        self._goals = goals
        self._report = report

    @staticmethod
    def create(bindings):
        from flatsurvey.pipeline import Goal
        return Worker(goals=bindings.get(Goal), report=bindings.get(Report))

    @classmethod
    async def work(cls, /, bindings: Bindings, limits=[]):
        worker = bindings.get(Worker)

        try:
            await worker.start(limits=limits)
        except Restart as restart:
            print("Performing restart")
            await Worker.work(bindings=restart.restart(bindings), limits=limits)

    async def start(self, limits=[]):
        r"""
        Run until all our goals are resolved.
        """

        def callback():
            for goal in self._goals:
                goal._resolved = True

        from flatsurvey.dask.limits import LimitChecker

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

        self._report.flush()


if __name__ == "__main__":
    worker()
