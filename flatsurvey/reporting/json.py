r"""
Writes results as JSON files.

EXAMPLES::

    >>> from flatsurvey.test.cli import invoke
    >>> from flatsurvey.worker import worker
    >>> invoke(worker, "json", "--help") # doctest: +NORMALIZE_WHITESPACE
    Usage: worker json [OPTIONS]
      Writes results in JSON format.
    Options:
      --output FILE              [default: derived from surface name]
      --prefix DIRECTORY
      --pickles / --no-pickles
      --help              Show this message and exit.

"""
# *********************************************************************
#  This file is part of flatsurvey.
#
#        Copyright (C) 2022-2025 Julian Rüth
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
from flatsurvey.pipeline import Bindings
from flatsurvey.reporting.reporter import Reporter
from flatsurvey.ui.group import GroupedCommand
from flatsurvey.surfaces import Surface


class Json(Reporter, Command):
    r"""
    Writes results in JSON format.

    EXAMPLES::

        >>> from flatsurvey.surfaces import Ngon
        >>> surface = Ngon((1, 1, 1))
        >>> Json(surface)
        json

        >>> TODO: Externalize pickles automatically.

    """

    # TODO: Generalize the "surface" here. We want to track any configuration for this survey, i.e., anything that is not a "result".
    # The logic that "Join" uses is that anything that does not map to a list is configuration.

    def __init__(self, surface: Surface, output=None, prefix=None, pickles=False):
        super().__init__()

        if prefix is not None:
            if output is not None:
                raise ValueError("at most one of output and prefix must be given")

            import os.path
            output = os.path.join(prefix, f"{surface.basename()}.json")

        if output is None:
            output = "-"

        self._output = output
        self._pickles = pickles

        self._data: dict = {"surface": surface}

    @staticmethod
    @click.command(
        name="json",
        cls=GroupedCommand,
        group="Reports",
        help=__doc__.split("EXAMPLES")[0],  # type: ignore
    )
    @click.option(
        "--output",
        type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
        default=None,
        help="[default: derived from surface name]",
    )
    @click.option(
        "--prefix",
        type=click.Path(exists=True, file_okay=False, dir_okay=True, allow_dash=False),
        default=None,
    )
    @click.option("--pickles/--no-pickles", default=False)
    @Bindings.click
    def click(bindings: Bindings, output, prefix, pickles):
        r"""
        Parse command line options into ``bindings``.

        TESTS::

            >>> from flatsurvey.test.cli import invoke_subcommand
            >>> invoke_subcommand(Json.click)

        """
        bindings.append(list[Reporter], Json)
        with bindings.scope(Json) as scoped:
            scoped.define(output=output, prefix=prefix, pickles=pickles)

    @staticmethod
    def create(bindings):
        r"""
        Create a JSON reporter from the configuration in the ``bindings``.

        TESTS::

            >>> from flatsurvey.pipeline import Bindings
            >>> from flatsurvey.test.cli import invoke_subcommand
            >>> from flatsurvey.surfaces import Ngon, Surface
            >>> bindings = Bindings()
            >>> invoke_subcommand(Json.click, bindings=bindings)
            >>> bindings.define(Surface, Ngon((1, 1, 1)))
            >>> Json.create(bindings)
            json

        """
        with bindings.scope(Json) as scoped:
            surface = bindings.get(Surface)
            output = scoped.get("output")
            prefix = scoped.get("prefix")
            pickles = scoped.get("pickles")

        return Json(surface, output=output, prefix=prefix, pickles=pickles)

    def deform(self, deformation):
        r"""
        Return a new logger that continues the previous logger's job after the
        underlying surface has been replaced with a ``deformation``.

        INPUT:

        - ``deformation`` -- a :class:`Surface`

        EXAMPLES:

        We want to write data about a deformed surface to the original
        surface's file so we do not change anything here. (The surface is only
        used to determine the file name, it's not written anywhere in the JSON
        file automatically.)::

            >>> from flatsurvey.surfaces import Ngon
            >>> surface = Ngon((1, 1, 1))

            >>> json = Json(surface)
            >>> json.deform(Ngon((1, 1, 2))) is json
            True

        """
        return self

    async def result(self, source, result, **kwargs):
        r"""
        Report a ``result`` for ``source``.

        EXAMPLES::

            >>> from flatsurvey.surfaces import Ngon
            >>> surface = Ngon((1, 1, 1))
            >>> json = Json(surface)

            >>> import asyncio
            >>> asyncio.run(json.result(source=None, result=True))
            >>> json.flush()
            {"surface": {"angles": [1, 1, 1], "type": "Ngon", "pickle": "dropped"}, "None": [{"timestamp": "...", "value": true}]}

        """
        from datetime import datetime, timezone

        result = self._simplify(
            result, **{"timestamp": str(datetime.now(timezone.utc)), **kwargs}
        )

        self._data.setdefault(str(source), [])
        self._data[str(source)].append(result)

    def _serialize_to_pickle(self, obj):
        r"""
        Return a JSON serializable version of ``obj``.

        Called for objects that cannot be otherwised encoded as JSON.

        EXAMPLES::

            >>> from flatsurvey.surfaces import Ngon
            >>> surface = Ngon((1, 1, 1))
            >>> json = Json(surface, pickles=True)

            >>> json._serialize_to_pickle(True)
            {'type': 'bool', 'pickle': 'gASILg=='}

            >>> TODO: Use the version in pickles.py instead.

            >>> TODO: Show dropped.

        """
        import base64
        from pickle import dumps

        characteristics = {}

        if hasattr(obj, "_flatsurvey_characteristics"):
            characteristics = obj._flatsurvey_characteristics()

        characteristics.setdefault("type", type(obj).__name__)
        characteristics.setdefault("repr", repr(obj))
        if self._pickles:
            characteristics.setdefault(
                "pickle", base64.encodebytes(dumps(obj)).decode("utf-8").strip()
            )

        return characteristics

    def _simplify_unknown(self, value):
        r"""
        Return the argument in a way that JSON serialization can make sense of.

        EXAMPLES:

        Anything that is unknown is rendered as its pickle, so we can let any
        object that we don't understand through without changes::

            >>> from flatsurvey.surfaces import Ngon
            >>> surface = Ngon((1, 1, 1))
            >>> json = Json(surface)

            >>> import asyncio
            >>> asyncio.run(json.result("verdict", result=asyncio))

            >>> json.flush()  # doctest: +ELLIPSIS
            {"surface": {"angles": [1, 1, 1], "type": "Ngon", "pickle": "..."}, "verdict": [{"timestamp": ..., "value": {"type": "module", "pickle": "..."}}]}

        """
        return value

    def flush(self):
        r"""
        Write reported data out as a JSON stream.

        EXAMPLES::

            >>> from flatsurvey.surfaces import Ngon
            >>> surface = Ngon((1, 1, 1))
            >>> json = Json(surface)

            >>> import asyncio
            >>> asyncio.run(json.result("verdict", result=True))

        Note that each result is reported individually, so the "verdict" is a list here::

            >>> json.flush()  # doctest: +ELLIPSIS
            {"surface": {"angles": [1, 1, 1], "type": "Ngon", "pickle": "..."}, "verdict": [{"timestamp": ..., "value": true}]}

        """
        import json
        import sys
        from contextlib import nullcontext

        with (
            open(self._output, "w") if self._output != "-" else nullcontext(sys.stdout)
        ) as stream:
            stream.write(json.dumps(self._data, default=self._serialize_to_pickle))
            stream.flush()

    @staticmethod
    def load(file) -> dict:
        r"""
        Load a JSON file into the cache dict with the fast orjson.
        """
        import orjson
        try:
            data = file.read().strip() or '{}'

            return orjson.loads(data)
        except Exception as e:
            print(f"Failed to parse {file}, {e}. Ignoring.")
            return {}


__test__ = {
    # doctests of Json.click do not run unless explicitly mentioned here due to the click decorator.
    "Json.click": Json.click.__doc__,
}
