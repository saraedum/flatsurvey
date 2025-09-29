r"""
The saddle connections on a translation surface.

    >>> from flatsurvey.test.cli import invoke
    >>> from flatsurvey.worker.worker import worker
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
from flatsurvey.pipeline import Producer
from flatsurvey.ui.group import GroupedCommand
from flatsurvey.surfaces import Surface
from flatsurvey.reporting import Report


class SaddleConnections(Producer, Command):
    r"""
    Saddle connections on the surface.
    """
    DEFAULT_BOUND = None
    DEFAULT_LIMIT = None

    def __init__(self, surface: Surface, report: Report, limit=DEFAULT_LIMIT, bound=DEFAULT_BOUND):
        super().__init__(report=report)

        self._surface = surface
        self._limit = limit
        self._bound = bound

        self._connections = None
        self._count = 0

    @staticmethod
    def create(bindings):
        with bindings.scope(SaddleConnections) as scoped:
            return SaddleConnections(
                surface=bindings.get(Surface),
                report=bindings.get(Report),
                limit=scoped.get("limit", default=lambda: SaddleConnections.DEFAULT_LIMIT),
                bound=scoped.get("bound", default=lambda: SaddleConnections.DEFAULT_BOUND),
            )

    def _by_length(self):
        self.__connections = (
            self._surface.surface().pyflatsurf().codomain().flat_triangulation().connections().byLength()
        )
        if self._bound is not None:
            self.__connections = self.__connections.bound(self._bound)
        if self._limit is not None:
            from itertools import islice

            self.__connections = islice(self.__connections, 0, self._limit)
        self._connections = iter(self.__connections)

    def randomize(self, lower_bound):
        self.__connections = (
            self._surface.surface().pyflatsurf().codomain().flat_triangulation()
            .connections()
            .sample()
            .lowerBound(lower_bound)
        )
        if self._bound is not None:
            raise NotImplementedError(
                "Cannot randomize saddle connections with --bound yet."
            )
        if self._limit is not None:
            from itertools import islice

            self.__connections = islice(self.__connections, 0, self._limit)
        self._connections = iter(self.__connections)

    def _produce(self):
        if self._connections is None:
            self._by_length()
        try:
            self._current = next(self._connections)

            self._report.progress(source=self, what="connections", count=self._count)
            return not Producer.EXHAUSTED
        except StopIteration:
            return Producer.EXHAUSTED

    @classmethod
    @click.command(
        name="saddle-connections",
        cls=GroupedCommand,
        group="Intermediates",
        help=__doc__.split("EXAMPLES")[0],
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
    def click(bound, limit):
        raise NotImplementedError
        return {
            "bindings": [
                PartialBindingSpec(SaddleConnections)(bound=bound, limit=limit)
            ]
        }
