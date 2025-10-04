r"""
The saddle connections on a translation surface.

    >>> from flatsurvey.test.cli import invoke
    >>> from flatsurvey.worker import worker
    >>> invoke(worker, "saddle-connections", "--help") # doctest: +NORMALIZE_WHITESPACE
    Usage: worker saddle-connections [OPTIONS]
      Saddle connections on the surface.
    Options:
      --bound INTEGER  stop search after all saddle connections up to that length
                       have been processed  [default: no bound]
      --limit INTEGER  stop search after that many saddle connections have been
                       considered  [default: no limit]
      --help           Show this message and exit.

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
from flatsurvey.pipeline import Producer, Bindings
from flatsurvey.ui.group import GroupedCommand
from flatsurvey.surfaces import Surface
from flatsurvey.reporting import Report


class SaddleConnections(Producer, Command):
    r"""
    Saddle connections on the surface.
    """
    DEFAULT_BOUND = None
    DEFAULT_LIMIT = None

    def __init__(self, surface: Surface, report: Report|None=None, limit=DEFAULT_LIMIT, bound=DEFAULT_BOUND):
        super().__init__(report=report)

        self._surface = surface
        self._limit = limit
        self._bound = bound

        self._count = 0

        # We initialize the connections lazily so we do not instantiate the
        # surface until it is requested. This helps against memory leaks when
        # resolving results from the cache.
        self.__connections = None
        self.__connections_iterator = None

    @staticmethod
    def create(bindings: Bindings):
        r"""
        Return a ``SaddleConnections`` instance from the configuration registered in ``bindings``.

        TESTS::

            >>> from flatsurvey.pipeline import Bindings
            >>> from flatsurvey.test.cli import invoke_subcommand
            >>> from flatsurvey.surfaces.ngons import Ngon
            >>> bindings = Bindings()
            >>> invoke_subcommand(Ngon.click, "-a", "1", "-a", "1", "-a", "1", bindings=bindings)
            >>> invoke_subcommand(SaddleConnections.click, bindings=bindings)
            >>> SaddleConnections.create(bindings)
            saddle-connections

        """
        with bindings.scope(SaddleConnections) as scoped:
            return SaddleConnections(
                surface=bindings.get(Surface),
                report=bindings.get(Report),
                limit=scoped.get("limit", default=lambda: SaddleConnections.DEFAULT_LIMIT),
                bound=scoped.get("bound", default=lambda: SaddleConnections.DEFAULT_BOUND),
            )

    @staticmethod
    @click.command(
        name="saddle-connections",
        cls=GroupedCommand,
        group="Intermediates",
        help=__doc__.split("EXAMPLES")[0],  # type: ignore
    )
    @click.option(
        "--bound",
        type=int,
        default=DEFAULT_BOUND,
        help="stop search after all saddle connections up to that length have been processed  [default: no bound]",
    )
    @click.option(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="stop search after that many saddle connections have been considered  [default: no limit]",
    )
    @Bindings.click
    def click(bindings: Bindings, bound, limit):
        r"""
        Parse command line options into ``bindings``.

        TESTS::

            >>> from flatsurvey.test.cli import invoke_subcommand
            >>> invoke_subcommand(SaddleConnections.click)

        """
        with bindings.scope(SaddleConnections) as scoped:
            scoped.define(bound=bound, limit=limit)

    def randomize(self, lower_bound):
        r"""
        Take the saddle connections produced from a random sample of
        connections of length at least ``lower_bound``. (Instead of normally
        taking them just by length increasing.)

        TESTS::

            >>> from flatsurvey.pipeline import Bindings
            >>> from flatsurvey.test.cli import invoke_subcommand
            >>> from flatsurvey.surfaces.ngons import Ngon
            >>> bindings = Bindings()
            >>> invoke_subcommand(Ngon.click, "-a", "1", "-a", "1", "-a", "1", bindings=bindings)
            >>> invoke_subcommand(SaddleConnections.click, bindings=bindings)
            >>> sc = SaddleConnections.create(bindings)

            >>> import asyncio
            >>> asyncio.run(sc.produce())
            'NOT_EXHAUSTED'

            >>> sc._current > 64
            False

        ::

            >>> sc.randomize(lower_bound=64)

            >>> import asyncio
            >>> asyncio.run(sc.produce())
            'NOT_EXHAUSTED'

            >>> sc._current > 64
            True

        """
        self._reset(
            self._surface.surface().pyflatsurf().codomain().flat_triangulation()
            .connections()
            .sample()
            .lowerBound(lower_bound)
        )

    def _reset(self, connections):
        r"""
        Reset the internal source of saddle connections to ``connections``.

        This is a helper to switch the saddle connections from being iterated
        by length increasing or randomly.
        """
        if self._bound is not None:
            connections = connections.bound(self._bound)

        if self._limit is not None:
            from itertools import islice
            connections = islice(connections, 0, self._limit)

        # We keep an explicit reference to the pyflatsurf object to avoid segfault due to too eager cleanup
        self.__connections = connections
        self.__connections_iterator = iter(self.__connections)

    def _produce(self):
        if self.__connections_iterator is None:
            self._reset(self._surface.surface().pyflatsurf().codomain().flat_triangulation().connections().byLength())

        assert self.__connections_iterator is not None

        try:
            self._current = next(self.__connections_iterator)
        except StopIteration:
            return "EXHAUSTED"

        self._count += 1

        self._report.progress(source=self, what="connections", count=self._count)
        return "NOT_EXHAUSTED"


__test__ = {
    # doctests of click do not run unless explicitly mentioned here due to the click decorator.
    "SaddleConnections.click": SaddleConnections.click.__doc__,
}
