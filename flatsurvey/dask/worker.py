r"""
A module that gets preloaded in each dask worker.

This module works around many oddities and performance problems in using
SageMath in dask. It also adds command line parameters to the workers that
allow us to enforce memory and runtime limits. Normally, the dask nanny would
enforce such limits but we cannot use the nanny with SageMath.

EXAMPLES::

    >>> from flatsurvey.test.cli import invoke
    >>> invoke(dask_setup, "--help")  # doctest: +NORMALIZE_WHITESPACE
    Usage: dask-setup [OPTIONS]
      A parser that we inject into the dask worker command line parser.
      Dask executes this ``dask_setup`` with command line arguments that it cannot
      make sense of since we preload this module into the dask worker.
      We use this to set global runtime limits that are stored in a global
      ``LIMITS`` variable.
    Options:
      --mem-limit TEXT   Gracefully stop a task when the memory consumption exceeds
                         this amount
      --time-limit TEXT  Gracefully stop a task when the wall time elapsed exceeds
                         this amount
      --help             Show this message and exit.

TESTS:

Importing this module silences cppyy warnings::

    >>> import cppyy

This module provides us with a worker-global forkserver with SageMath preloaded
that is used by the :class:`Runner`.

    >>> forkserver
    <multiprocessing.context.ForkServerContext object at 0x...>

"""
# *********************************************************************
#  This file is part of flatsurvey.
#
#        Copyright (C) 2024-2025 Julian Rüth
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
import multiprocessing

import click

# We import sage.all before forking off child processes.
# Importing sage.all is very costly, so we import it once in the template
# process that gets then cloned to produce the workers in the fork calls.
forkserver = multiprocessing.get_context("forkserver")
multiprocessing.set_forkserver_preload(["sage.all"])

# Silence warnings from cppyy which is still relying on pkg_resources.
import warnings
warnings.filterwarnings('ignore', module='cppyy', message='pkg_resources is deprecated as an API')



@click.command()
@click.option(
    # We cannot call this --memory-limit because dask-worker uses this already.
    "--mem-limit",
    default=None,
    help="Gracefully stop a task when the memory consumption exceeds this amount",
)
@click.option(
    "--time-limit",
    default=None,
    help="Gracefully stop a task when the wall time elapsed exceeds this amount",
)
def dask_setup(worker, mem_limit, time_limit):
    r"""
    A parser that we inject into the dask worker command line parser.

    Dask executes this ``dask_setup`` with command line arguments that it
    cannot make sense of since we preload this module into the dask worker.

    We use this to set global runtime limits that are stored in a global
    ``LIMITS`` variable.
    """
    if mem_limit is not None:
        from flatsurvey.dask.limits import MemoryLimit
        from flatsurvey.dask.task import Task

        Task.LIMITS.append(MemoryLimit(MemoryLimit.parse_limit(mem_limit)))

    if time_limit is not None:
        from flatsurvey.dask.limits import TimeLimit
        from flatsurvey.dask.task import Task

        Task.LIMITS.append(TimeLimit(TimeLimit.parse_limit(time_limit)))
