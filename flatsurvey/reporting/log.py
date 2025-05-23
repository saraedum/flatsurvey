r"""
Writes progress and results as an unstructured log file.

EXAMPLES::

    >>> from flatsurvey.test.cli import invoke
    >>> from flatsurvey.worker.worker import worker
    >>> invoke(worker, "log", "--help") # doctest: +NORMALIZE_WHITESPACE
    Usage: worker log [OPTIONS]
      Writes progress and results as an unstructured log file.
    Options:
      --output FILE      [default: stdout]
      --prefix DIRECTORY
      --help             Show this message and exit.

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
from flatsurvey.pipeline import Pipeline
from flatsurvey.reporting.reporter import Reporter
from flatsurvey.ui.group import GroupedCommand
from flatsurvey.surfaces import Surface


class BaseLog(Reporter, Command):
    def __init__(self, stream=None):
        if stream is None:
            import sys
            stream = sys.stdout

        self._stream = stream

    def _log(self, message):
        self._stream.write("%s\n" % (message,))
        self._stream.flush()

    def _log_prefix(self, source):
        return f"[{type(source).__name__}]"

    def log(self, source, message, **kwargs):
        r"""
        Write a ``message`` to the log.

        EXAMPLES::

            >>> from flatsurvey.surfaces import Ngon
            >>> surface = Ngon((1, 1, 1))

            >>> log = Log(surface)
            >>> log.log(source=surface, message="Hello World", extra="data", lot="1337")
            [Ngon([1, 1, 1])] [Ngon] Hello World (extra: data) (lot: 1337)

        """
        message = f"{self._log_prefix(source)} {message}"
        for k, v in kwargs.items():
            message += f" ({k}: {v})"
        self._log(message)

    async def result(self, source, result, **kwargs):
        r"""
        Report a result to the log.

        EXAMPLES::

            >>> from flatsurvey.surfaces import Ngon
            >>> surface = Ngon((1, 1, 1))

            >>> import asyncio
            >>> log = Log(surface)
            >>> result = log.result(source=surface, result="dense orbit closure", dimension=1337)
            >>> asyncio.run(result)
            [Ngon([1, 1, 1])] [Ngon] dense orbit closure (dimension: 1337)
            >>> result = log.result(source=surface, result=None)
            >>> asyncio.run(result)
            [Ngon([1, 1, 1])] [Ngon] ¯\_(ツ)_/¯

        """
        shruggie = r"¯\_(ツ)_/¯"
        result = shruggie if result is None else str(result)
        if kwargs.pop("cached", False):
            result = f"{result} (cached)"
        self.log(source, result, **kwargs)

    def progress(
        self,
        source,
        count=None,
        advance=None,
        what=None,
        total=None,
        message=None,
        parent=None,
        activity=None,
    ):
        r"""
        Write a progress update to the log.

        EXAMPLES::

            >>> from flatsurvey.surfaces import Ngon
            >>> surface = Ngon((1, 1, 1))

            >>> log = Log(surface)
            >>> log.progress(source=surface, what='progress', count=10, total=100)
            [Ngon([1, 1, 1])] [Ngon] progress: 10/100
            >>> log.progress(source=surface, what='dimension', count=10)
            [Ngon([1, 1, 1])] [Ngon] dimension: 10/?

        """
        if advance is not None:
            return

        if count is not None and what is not None:
            line = f"{what}: {count}/{total or '?'}"
            if message:
                line = f"{line} {message}"
        elif message is not None:
            line = message
        else:
            return

        self.log(source, line)


class Log(BaseLog):
    # TODO: Extract a non-surface log as a base class and use it for the maintenance goals.
    r"""
    Writes progress and results as an unstructured log file.

    EXAMPLES::

        >>> from flatsurvey.surfaces import Ngon
        >>> surface = Ngon((1, 1, 1))

        >>> log = Log(surface)
        >>> log.log(source=surface, message="Hello World")
        [Ngon([1, 1, 1])] [Ngon] Hello World

    """

    def __init__(self, surface: Surface, stream=None, output=None, prefix=None):
        self._surface = surface

        if prefix is not None:
            if output is not None:
                raise ValueError("at most one of stream, output, prefix must be given")
            
            import os.path
            output = os.path.join(prefix, f"{surface.basename()}.log")

        if output is not None:
            if stream is not None:
                raise ValueError("at most one of stream, output, prefix must be given")

            stream = open(output, "w")

        super().__init__(stream=stream)

    def deform(self, deformation):
        return Log(surface=deformation, stream=self._stream)

    def _log_prefix(self, source):
        return f"[{self._surface}] [{type(source).__name__}]"

    @staticmethod
    @click.command(
        name="log",
        cls=GroupedCommand,
        group="Reports",
        help=__doc__.split("EXAMPLES")[0],
    )
    @click.option(
        "--output",
        type=click.Path(file_okay=True, dir_okay=False, allow_dash=True),
        default=None,
        help="[default: stdout]",
    )
    @click.option(
        "--prefix",
        type=click.Path(exists=True, file_okay=False, dir_okay=True, allow_dash=False),
        default=None,
    )
    @Pipeline.click
    def click(pipeline: Pipeline, output, prefix):
        pipeline.append("reporters", Log)
        pipeline.define(
            scope=Log,
            output=output,
            prefix=prefix)

    @staticmethod
    def create(pipeline: Pipeline):
        with pipeline.scope(Log) as get:
            return Log(
                surface=get(Surface),
                stream=get("stream", None),
                output=get("output", None),
                prefix=get("prefix", None))
