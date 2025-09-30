r"""
The saddle connections on a translation surface.

    >>> from flatsurvey.test.cli import invoke
    >>> from flatsurvey.worker.worker import worker
    >>> invoke(worker, "saddle-connection-orientations", "--help") # doctest: +NORMALIZE_WHITESPACE
    Usage: worker saddle-connection-orientations [OPTIONS]
      Orientations of saddle connections on the surface, i.e., the vectors of
      saddle connections irrespective of scaling and sign.
    Options:
      --help  Show this message and exit.

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

from flatsurvey.ui import Command
from flatsurvey.pipeline import Processor
from flatsurvey.ui.group import GroupedCommand
from flatsurvey.jobs.saddle_connections import SaddleConnections
from flatsurvey.reporting import Report


class SaddleConnectionOrientations(Processor, Command):
    r"""
    Orientations of saddle connections on the surface, i.e., the vectors of
    saddle connections irrespective of scaling and sign.
    """

    def __init__(self, saddle_connections: SaddleConnections, report: Report):
        super().__init__(producers=[saddle_connections], report=report)

        self._saddle_connections = saddle_connections

        self._seen = None

    @staticmethod
    def create(bindings):
        return SaddleConnectionOrientations(
            saddle_connections=bindings.get(SaddleConnections),
            report=bindings.get(Report)
        )

    async def _consume(self, product, cost):
        import cppyy

        vector = product.vector()
        if self._seen == None:
            self._seen = cppyy.gbl.std.set[type(vector), type(vector).CompareSlope]()

        if vector.x():
            try:
                vector = type(vector)(vector.x() / vector.x(), vector.y() / vector.x())
            except Exception:
                pass
        if vector.y():
            try:
                vector = type(vector)(vector.x() / vector.y(), vector.y() / vector.y())
            except Exception:
                pass

        # TODO: What is this good for (unused)?
        flat_triangulation = self._saddle_connections._surface.surface().pyflatsurf().codomain().flat_triangulation()
        cppyy.gbl.flatsurf.Vertex.source(
            product.source(), flat_triangulation.combinatorial()
        )
        cppyy.gbl.flatsurf.Vertex.source(
            product.target(), flat_triangulation.combinatorial()
        )

        if self._seen.find(vector) == self._seen.end():
            self._seen.insert(vector)
            self._current = product.vector()
            self._current = type(self._current)(self._current)
            await self._notify_consumers(cost)

        return "NOT_COMPLETED"

    @classmethod
    @click.command(
        name="saddle-connection-orientations",
        cls=GroupedCommand,
        group="Intermediates",
        help=__doc__.split("EXAMPLES")[0],
    )
    def click():
        return {"bindings": SaddleConnectionOrientations}
