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

    We use this to set global runtime limits that are stored in the global
    ``limits`` variable in this module.
    """
    global limits
    if mem_limit is not None:
        from flatsurvey.dask import MemoryLimit

        DaskTask.LIMITS.append(MemoryLimit(MemoryLimit.parse_limit(mem_limit)))

    if time_limit is not None:
        from flatsurvey.dask import TimeLimit

        DaskTask.LIMITS.append(TimeLimit(TimeLimit.parse_limit(time_limit)))
