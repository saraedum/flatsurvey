r"""
Helpers for click CLI testing.

Click's own CliRunner is quite cumbersome to work with in some simple test
scenarios so we wrap it in more convenient ways here.

"""
# *********************************************************************
#  This file is part of flatsurvey.
#
#        Copyright (C) 2020-2025 Julian Rüth
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

import click
from click.testing import CliRunner

from flatsurvey.pipeline import Bindings


def invoke(command, *args):
    r"""
    Invoke the click ``command`` with the given list of string arguments.

    >>> import click
    >>> @click.command()
    ... def hello(): print("Hello World")
    >>> invoke(hello)
    Hello World

    >>> @click.command()
    ... def fails(): raise Exception("expected error")
    >>> invoke(fails)
    Traceback (most recent call last):
    ...
    Exception: expected error

    """
    invocation = CliRunner().invoke(command, args, catch_exceptions=False)
    output = invocation.output.strip()
    if output:
        print(output)


def invoke_subcommand(command, *args, bindings: Bindings | None=None):
    if bindings is None:
        bindings = Bindings()

    @click.group(chain=True)
    def doctest():
        pass

    doctest.add_command(command)

    @doctest.result_callback()
    def process(commands):
        for command in commands:
            command(bindings)

    invocation = CliRunner().invoke(doctest, (command.name,) + args, catch_exceptions=False)
    output = invocation.output.strip()
    if output:
        print(output)
