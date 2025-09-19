r"""
Access a database of pickles storing parts of previous computations.

This modules supplements :module:`flatsurvey.cache.Cache`. The cache in JSON
files contains references to pickles. These pickles are resolved here.

Currently, this is mostly a placeholder. We have lots of pickles from previous
runs but unpickling them is not implemented in much generality, see #10.
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

from flatsurvey.ui import Command, GroupedCommand


class Pickles(Command):
    def __init__(self, providers=()):
        self._providers = providers

    @classmethod
    @click.command(
        name="pickles",
        cls=GroupedCommand,
        group="Cache",
        help=__doc__.split("EXAMPLES")[0],
    )
    @click.option(
        "--dir",
        "-d",
        metavar="PATH",
        multiple=True,
        type=str,
        help="local directory to search for pickles",
    )
    def click(dir):
        providers = [DirectoryPickleProvider(d) for d in dir]

        return {
            "bindings": Pickles.bindings(providers=providers),
        }

    @classmethod
    def bindings(cls, providers):
        raise NotImplementedError
        return [PartialBindingSpec(Pickles)(providers=providers)]

    def unpickle(self, pickle, kind):
        for provider in self._providers:
            try:
                unpickled = provider.unpickle(pickle, kind)
            except KeyError:
                continue

            return unpickled

        raise ValueError(f"No pickle for {pickle} found")


class PickleProvider:
    def load(self, raw):
        # Work around some current problems in many of our pickles:
        # - Pickles import sage.rings.number_field but SageMath cannot handle
        #   this so we get a circular import. See #10.
        from pickle import loads

        # - Pickles use cppyy.gbl.flatsurf but it's not available yet somehow. See #10.
        import pyflatsurf
        import sage.all

        try:
            return loads(raw)
        except Exception:
            raise Exception(f"Failed to unpickle {raw}")


class StaticPickleProvider(PickleProvider):
    def __init__(self, data, digest=None):
        self._pickle = data

        from hashlib import sha256

        if digest is None:
            sha = sha256()
            sha.update(data)
            digest = sha.hexdigest()

        self._digest = digest

    def unpickle(self, digest, kind):
        if digest == self._digest:
            return self.load(self._pickle)
        raise KeyError(digest)


class DirectoryPickleProvider(PickleProvider):
    def __init__(self, path):
        raise NotImplementedError("DirectoryPickleProvider not implemented yet")


class GitHubPickleProvider(PickleProvider):
    def __init__(self, organization, project):
        raise NotImplementedError("GitHubPickleProvider not implemented yet")
