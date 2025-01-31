r"""
Computes flow decompositions of a flat triangulation into cylinders and minimal components.

Usually you do not need to interact with this module directly. Flow
decompositions are created by the bits of the computation that need them on
demand.

However, you can still change some of the behaviour of this module through the
`flow-decompositions` command, e.g., to set a different `--limit` for the
number of Zorich induction steps:

    >>> from flatsurvey.test.cli import invoke
    >>> from flatsurvey.worker.__main__ import worker
    >>> invoke(worker, "flow-decompositions", "--help") # doctest: +NORMALIZE_WHITESPACE
    Usage: worker flow-decompositions [OPTIONS]
      Turns directions coming from saddle connections into flow decompositions.
    Options:
      --limit INTEGER  Zorich induction steps to perform before giving up  [default: 256]
      --help           Show this message and exit.

"""
# *********************************************************************
#  This file is part of flatsurvey.
#
#        Copyright (C) 2020-2021 Julian Rüth
#
#  Flatsurvey is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Flatsurvey is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with flatsurvey. If not, see <https://www.gnu.org/licenses/>.
# *********************************************************************

import time

import click
from pinject import copy_args_to_internal_fields

from flatsurvey.pipeline.util import PartialBindingSpec
from flatsurvey.ui.group import GroupedCommand

from ..pipeline import Consumer, Processor


class FlowDecompositions(Processor):
    r"""
    Turns directions coming from saddle connections into flow decompositions.

    EXAMPLES::

        >>> from flatsurvey.surfaces import Ngon
        >>> from flatsurvey.reporting import Report
        >>> from flatsurvey.jobs import SaddleConnectionOrientations, SaddleConnections
        >>> surface = Ngon((1, 1, 1))
        >>> FlowDecompositions(surface=surface, report=Report([]), saddle_connection_orientations=SaddleConnectionOrientations(SaddleConnections(surface)))
        flow-decompositions

    """
    DEFAULT_LIMIT = 256

    @copy_args_to_internal_fields
    def __init__(
        self, surface, report, saddle_connection_orientations, limit=DEFAULT_LIMIT
    ):
        super().__init__(producers=[saddle_connection_orientations])

    @classmethod
    @click.command(
        name="flow-decompositions",
        cls=GroupedCommand,
        group="Intermediates",
        help=__doc__.split("EXAMPLES:")[0],
    )
    @click.option(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        show_default=True,
        help="Zorich induction steps to perform before giving up",
    )
    def click(limit):
        return {
            # TODO: This should not be a goal but something else. Do the same for other processors. See #9.
            "goals": [FlowDecompositions],
            "bindings": [PartialBindingSpec(FlowDecompositions)(limit=limit)],
        }

    def command(self):
        command = ["flow-decompositions"]
        if self._limit != self.DEFAULT_LIMIT:
            command.append(f"--limit={self._limit}")
        return command

    async def _consume(self, orientation, cost):
        r"""
        Produce the flow decomposition corresponding to ``orientation``.

        EXAMPLES::

            >>> import asyncio
            >>> from flatsurvey.surfaces import Ngon
            >>> from flatsurvey.reporting import Log, Report
            >>> from flatsurvey.jobs import SaddleConnectionOrientations, SaddleConnections
            >>> surface = Ngon((1, 1, 1))
            >>> decompositions = FlowDecompositions(surface=surface, report=Report([Log(surface)]), saddle_connection_orientations=SaddleConnectionOrientations(SaddleConnections(surface)))
            >>> produce = decompositions.produce() # indirect doctest
            >>> asyncio.run(produce)
            [Ngon([1, 1, 1])] [FlowDecompositions] ¯\_(ツ)_/¯ (orientation: (-6, -(2*c ~ 3.4641016))) (cylinders: 1) (minimal: 0) (undetermined: 0)
            True
            >>> decompositions._current
            Flow decomposition with 1 cylinders, 0 minimal components and 0 undetermined components

        """
        start = time.perf_counter()
        self._current = self._surface.orbit_closure().decomposition(
            orientation, self._limit
        )
        cost += time.perf_counter() - start

        await self._report.result(
            self,
            # flatsurf::FlowDecomposition cannot be serialized yet: https://github.com/flatsurf/flatsurf/issues/274
            # self._current.decomposition,
            None,
            orientation=orientation,
            cylinders=len(self._current.decomposition.cylinders()),
            minimal=len(self._current.decomposition.minimalComponents()),
            undetermined=len(self._current.decomposition.undeterminedComponents()),
        )

        await self._notify_consumers(cost)

        return not Processor.EXHAUSTED
