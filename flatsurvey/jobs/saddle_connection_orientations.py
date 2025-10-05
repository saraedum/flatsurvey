r"""
The saddle connection directions on a translation surface module scaling.

    >>> from flatsurvey.test.cli import invoke
    >>> from flatsurvey.worker import worker
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
from flatsurvey.pipeline import Processor, Bindings
from flatsurvey.ui.group import GroupedCommand
from flatsurvey.jobs.saddle_connections import SaddleConnections
from flatsurvey.reporting import Report


class SaddleConnectionOrientations(Processor, Command):
    r"""
    Orientations of saddle connections on the surface, i.e., the vectors of
    saddle connections irrespective of scaling and sign.
    """

    def __init__(self, saddle_connections: SaddleConnections, report: Report|None=None):
        super().__init__(producers=[saddle_connections], report=report)

        self._saddle_connections = saddle_connections

        self._seen = None

    @staticmethod
    def create(bindings: Bindings):
        r"""
        Return a ``SaddleConnectionOrientations`` instance from the configuration registered in ``bindings``.

        TESTS::

            >>> from flatsurvey.pipeline import Bindings
            >>> from flatsurvey.test.cli import invoke_subcommand
            >>> from flatsurvey.surfaces.ngons import Ngon
            >>> bindings = Bindings()
            >>> invoke_subcommand(Ngon.click, "-a", "1", "-a", "1", "-a", "1", bindings=bindings)
            >>> invoke_subcommand(SaddleConnectionOrientations.click, bindings=bindings)
            >>> SaddleConnectionOrientations.create(bindings)
            saddle-connection-orientations

        """
        return SaddleConnectionOrientations(
            saddle_connections=bindings.get(SaddleConnections),
            report=bindings.get(Report)
        )

    @staticmethod
    @click.command(
        name="saddle-connection-orientations",
        cls=GroupedCommand,
        group="Intermediates",
        help=__doc__.split("EXAMPLES")[0],  # type: ignore
    )
    @Bindings.click
    def click(bindings: Bindings):
        r"""
        Parse command line options into ``bindings``.

        TESTS::

            >>> from flatsurvey.test.cli import invoke_subcommand
            >>> invoke_subcommand(SaddleConnectionOrientations.click)

        """
        del bindings

    async def _consume(self, product, cost):
        r"""
        Turn a saddle connection into its orientation.

        EXAMPLES::

            >>> import asyncio
            >>> from flatsurvey.surfaces import Ngon, Surface
            >>> from flatsurvey.reporting import Log, Report
            >>> from flatsurvey.jobs import SaddleConnections
            >>> surface = Ngon((1, 1, 1))
            >>> sco = SaddleConnectionOrientations(saddle_connections=SaddleConnections(surface))

            >>> asyncio.run(sco.produce())  # doctest: +ELLIPSIS
            'NOT_EXHAUSTED'

        Check that the JSON output works::

            >>> from flatsurvey.reporting import Json

            >>> report = Report([Json({Surface: surface}, output="-")], ignore=["saddle-connections"])
            >>> sco = SaddleConnectionOrientations(saddle_connections=SaddleConnections(surface), report=report)

            >>> asyncio.run(sco.produce())
            'NOT_EXHAUSTED'

            >>> report.flush()
            {"surface": {...}, "saddle-connection-orientations": [{"timestamp": "...", "value": {"type": "Vector<eantic::renf_elem_class>", ...}}]}

        """
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

        if self._seen.find(vector) == self._seen.end():
            self._seen.insert(vector)
            self._current = product.vector()
            self._current = type(self._current)(self._current)

            await self._report.result(self, self._current)

            await self._notify_consumers(cost)

        return "NOT_COMPLETED"
