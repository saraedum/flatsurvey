r"""
Entrypoint to organize .JSON files written by surveys.

TESTS::

    >>> from flatsurvey.test.cli import invoke
    >>> invoke(cli) # doctest: +NORMALIZE_WHITESPACE
    Usage: cli [OPTIONS] COMMAND1 [ARGS]... [COMMAND2 [ARGS]...]...
    <BLANKLINE>
      Mangle cache files.
    <BLANKLINE>
    Options:
      --debug
      --help         Show this message and exit.
      -v, --verbose  Enable verbose message, repeat for debug message.
    <BLANKLINE>
    Commands:
      externalize-pickles
      join
      split

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

import flatsurvey.reporting.log
from flatsurvey.cache.externalize_pickles import ExternalizePickles
from flatsurvey.cache.join import Join
from flatsurvey.cache.split import Split
from flatsurvey.ui.group import CommandWithGroups


@click.group(
    chain=True,
    cls=CommandWithGroups,
    help=r"""Mangle cache files.""",
)
@click.option("--debug", is_flag=True)
@click.option(
    "--verbose",
    "-v",
    count=True,
    help="Enable verbose message, repeat for debug message.",
)
def cli(debug, verbose):
    r"""
    Performs maintenance tasks on collections of .JSON files.

    Specific tasks are registered as subcommands.
    """


cli.add_command(Split.click)
cli.add_command(Join.click)
cli.add_command(ExternalizePickles.click)


from flatsurvey.reporting.reporter import Reporter
class Log(Reporter):
    @staticmethod
    def create(pipeline):
        return Log()

    async def result(self, source, result, **kwargs):
        print(source, result, kwargs)

    def log(self, source, message, **kwargs):
        print(source, message, kwargs)


@cli.result_callback()
def process(commands, debug, verbose):
    r"""
    Run the specified subcommands of ``cli``.

    EXAMPLES:

    TODO
    """
    if debug:
        import pdb
        import signal

        signal.signal(signal.SIGUSR1, lambda sig, frame: pdb.Pdb().set_trace(frame))

    if verbose:
        import logging

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG if verbose > 1 else logging.INFO)

    from flatsurvey.pipeline import Pipeline

    pipeline = Pipeline()

    from flatsurvey.reporting.report import Report
    pipeline.append("reporters", Log)

    for command in commands:
        command(pipeline)

    try:
        import asyncio

        from flatsurvey.worker.worker import Worker
        asyncio.run(Worker.work(pipeline=pipeline, limits=[]))
    except Exception:
        if debug:
            pdb.post_mortem()
        raise
