r"""
Writes progress and results as an unstructured log file.

EXAMPLES::

    >>> from flatsurvey.test.cli import invoke
    >>> from flatsurvey.worker import worker
    >>> invoke(worker, "log", "--help") # doctest: +NORMALIZE_WHITESPACE
    Usage: worker log [OPTIONS]
      Writes progress and results as an unstructured log file.
    Options:
      --output FILE      [default: stdout]
      --prefix DIRECTORY
      --help             Show this message and exit.

::

    >>> from flatsurvey.cache.maintenance import cli as maintenance
    >>> invoke(maintenance, "log", "--help") # doctest: +NORMALIZE_WHITESPACE
    Usage: cli log [OPTIONS]
      Writes progress and results as an unstructured log file.
      Unlike :class:`Log` this is a generic text log writer that can be used outside
      surveys (e.g. by maintenance tasks.)
    Options:
      --output FILE  [default: stdout]
      --help         Show this message and exit.

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
from flatsurvey.pipeline import Bindings
from flatsurvey.reporting.reporter import Reporter
from flatsurvey.ui.group import GroupedCommand
from flatsurvey.surfaces import Surface


class GenericLog(Reporter, Command):
    r"""
    Writes progress and results as an unstructured log file.

    Unlike :class:`Log` this is a generic text log writer that can be used
    outside surveys (e.g. by maintenance tasks.)

    EXAMPLES::

        >>> from flatsurvey.reporting.log import GenericLog
        >>> GenericLog()
        log

    """
    def __init__(self, output=None, stream=None):
        super().__init__()

        if output is not None:
            if stream is not None:
                raise ValueError("at most one of stream or output must be given")

            stream = open(output, "w")

        if stream is None:
            import sys
            stream = sys.stdout

        self._stream = stream

    @staticmethod
    @click.command(
        name="log",
        cls=GroupedCommand,
        group="Reports",
        help=__doc__.split("EXAMPLES")[0],  # type: ignore
    )
    @click.option(
        "--output",
        type=click.Path(file_okay=True, dir_okay=False, allow_dash=True),
        default=None,
        help="[default: stdout]",
    )
    @Bindings.click
    def click(bindings: Bindings, output):
        r"""
        Parse command line options into ``bindings``.

        TESTS::

            >>> from flatsurvey.test.cli import invoke_subcommand
            >>> invoke_subcommand(GenericLog.click)

        """
        with bindings.scope(GenericLog) as scoped:
            scoped.define(output=output)

    @staticmethod
    def create(bindings: Bindings):
        r"""
        Create an instance of this class from the ``bindings`` (that have been
        typically set by :meth:`click`.)

        TESTS::

            >>> from flatsurvey.pipeline import Bindings
            >>> from flatsurvey.test.cli import invoke_subcommand
            >>> bindings = Bindings()
            >>> invoke_subcommand(GenericLog.click, bindings=bindings)
            >>> GenericLog.create(bindings)
            log

        """
        with bindings.scope(GenericLog) as scoped:
            return GenericLog(
                output=scoped.get("output", lambda: None))

    def _log(self, message):
        r"""
        Writes a ``message`` to the underlying log stream.

        TESTS::

            >>> from flatsurvey.reporting.log import GenericLog
            >>> log = GenericLog()
            >>> log._log("message")
            message

        """
        self._stream.write("%s\n" % (message,))
        self._stream.flush()

    def _log_prefix(self, source):
        r"""
        Return the prefix to use for each log message coming from ``source``.

        TESTS::

            >>> from flatsurvey.reporting.log import GenericLog
            >>> log = GenericLog()
            >>> log._log_prefix(log)
            '[GenericLog]'

        """
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
        what=None,
        total=None,
        message=None,
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
        if count is not None and what is not None:
            line = f"{what}: {count}/{total or '?'}"
            if message:
                line = f"{line} {message}"
        elif message is not None:
            line = message
        else:
            return

        self.log(source, line)


class Log(GenericLog):
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
        r"""
        Return a new logger that continues the previous logger's job after the
        underlying surface has been replaced with a ``deformation``.

        INPUT:

        - ``deformation`` -- a :class:`Surface`

        EXAMPLES::

            >>> from flatsurvey.surfaces import Ngon
            >>> surface = Ngon((1, 1, 1))

            >>> log = Log(surface)
            >>> log.log(source=surface, message="Hello World")
            [Ngon([1, 1, 1])] [Ngon] Hello World

            >>> log = log.deform(Ngon((1, 1, 2)))
            >>> log.log(source=surface, message="Hello World")
            [Ngon([1, 1, 2])] [Ngon] Hello World

        """
        return Log(surface=deformation, stream=self._stream)

    def _log_prefix(self, source):
        r"""
        Return the prefix to use for each log message coming from ``source``.

        TESTS::

            >>> from flatsurvey.surfaces import Ngon
            >>> surface = Ngon((1, 1, 1))

            >>> log = Log(surface)
            >>> log._log_prefix(log)
            '[Ngon([1, 1, 1])] [Log]'

        """
        return f"[{self._surface}] [{type(source).__name__}]"

    @staticmethod
    @click.command(
        name="log",
        cls=GroupedCommand,
        group="Reports",
        help=__doc__.split("EXAMPLES")[0],  # type: ignore
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
    @Bindings.click
    def click(bindings: Bindings, output, prefix):
        r"""
        Parse command line options into ``bindings``.

        TESTS::

            >>> from flatsurvey.test.cli import invoke_subcommand
            >>> invoke_subcommand(Log.click)

        """
        bindings.append("reporters", Log)
        bindings.define(
            scope=Log,
            output=output,
            prefix=prefix)

    @staticmethod
    def create(bindings: Bindings):
        r"""
        Create an instance of this class from the ``bindings`` (that have been
        typically set by :meth:`click`.)

        TESTS::

            >>> from flatsurvey.pipeline import Bindings
            >>> from flatsurvey.test.cli import invoke_subcommand
            >>> from flatsurvey.surfaces import Ngon, Surface
            >>> bindings = Bindings()
            >>> invoke_subcommand(Log.click, bindings=bindings)
            >>> bindings.define(Surface, Ngon((1, 1, 1)))
            >>> Log.create(bindings)
            log

        """
        with bindings.scope(Log) as scoped:
            return Log(
                surface=scoped.get(Surface),
                stream=scoped.get("stream", lambda: None),
                output=scoped.get("output", lambda: None),
                prefix=scoped.get("prefix", lambda: None))


__test__ = {
    # doctests of .click do not run unless explicitly mentioned here due to the click decorator.
    "GenericLog.click": GenericLog.click.__doc__,
    "Log.click": Log.click.__doc__,
}
