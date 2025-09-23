r"""
Aggregate cache files.

Combines JSON files that are produced by the
:class:`flatsurvey.reporting.json.Json` reporter into files grouped by topic.

EXAMPLES::

    >>> from flatsurvey.test.cli import invoke
    >>> from flatsurvey.cache.maintenance import cli
    >>> invoke(cli, "join", "--help")  # doctest: +NORMALIZE_WHITESPACE
    Usage: cli join [OPTIONS] [JSONS]...
      Aggregates JSON files into one file for each type of result.
    Options:
      --outdir PATH  a directory to write the output files to  [required]
      --help         Show this message and exit.

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

from pathlib import Path

import click

from flatsurvey.ui import Command
from flatsurvey.pipeline import Goal, Bindings


class Join(Goal, Command):
    r"""
    Aggregates JSON files into one file for each type of result.

    INPUT:

    - ``jsons`` -- a list of paths of existing JSON files.

    - ``outdir`` -- the output directory to write the JSON files to.

    EXAMPLES::

        >>> Join(jsons=[], outdir=Path("/tmp/"))
        join

    """

    def __init__(self, jsons: list[Path], outdir: Path):
        super().__init__()

        self._jsons = jsons
        self._outdir = outdir

    @staticmethod
    @click.command(name="join", help=__doc__.split("INPUT")[0])  # type: ignore
    @click.argument("jsons", nargs=-1, type=click.Path(exists=True))
    @click.option(
        "--outdir", type=click.Path(), required=True, help="a directory to write the output files to"
    )
    @Bindings.click
    def click(bindings: Bindings, jsons, outdir):
        r"""
        Parse command line options into ``bindings``.

        TESTS::

            >>> from flatsurvey.test.cli import invoke_subcommand
            >>> invoke_subcommand(Join.click, "--outdir=/tmp")

        """
        bindings.append("goals", Join)
        bindings.define(scope=Join, jsons=jsons, outdir=outdir)

    @staticmethod
    def create(bindings: Bindings):
        r"""
        Return a ``Join`` instance from the configuration registered in
        ``bindings``.

        TESTS::

            >>> from flatsurvey.pipeline import Bindings
            >>> from flatsurvey.test.cli import invoke_subcommand
            >>> bindings = Bindings()
            >>> invoke_subcommand(Join.click, "--outdir=/tmp", bindings=bindings)
            >>> Join.create(bindings)
            join

        """
        with bindings.scope(Join) as scoped:
            return Join(
                jsons=scoped.get("jsons"),
                outdir=scoped.get("outdir"))

    def _resolve_parsed_data(self):
        r"""
        Helper method for :meth:`resolve` that presents the JSON input as an
        iterator over parsed dicts.

        EXAMPLES::

            >>> from pathlib import Path
            >>> from tempfile import TemporaryDirectory

            >>> with TemporaryDirectory() as tmpdir:
            ...     tmpdir = Path(tmpdir)
            ...     with open(tmpdir / "a.json", "w") as json: _ = json.write('{"subject": {"result": true}}')
            ...     with open(tmpdir / "b.json", "w") as json: _ = json.write('{"subject": {"result": false}}')
            ...     join = Join(jsons=[tmpdir / "a.json", tmpdir / "b.json"], outdir=tmpdir)
            ...     list(join._resolve_parsed_data())
            [{'subject': {'result': True}}, {'subject': {'result': False}}]

        """
        for json in self._jsons:
            from flatsurvey.reporting.json import Json
            with open(json, "r") as input:
                yield Json.load(input)

    @staticmethod
    def _resolve_create_subjects(parsed):
        r"""
        Helper method for :meth:`resolve` that rewrites the dict ``parsed``
        into a subject centered dict.

        EXAMPLES::

            >>> Join._resolve_create_subjects({})
            {}

        Survey configuration is copied into each result::

            >>> Join._resolve_create_subjects({
            ...     "surface": "some-surface",
            ...     "seed": 1337,
            ...     "orbit-closure": [{"dense": None}, {"dense": True}]
            ... })
            {'orbit-closure': [{'surface': 'some-surface', 'seed': 1337, 'dense': None}, {'surface': 'some-surface', 'seed': 1337, 'dense': True}]}

        Note that anything that isn't a list is considered configuration, see :meth:`Json.result`.

        """
        configuration = {}
        subjects = {}

        for key, value in parsed.items():
            if isinstance(value, list):
                subjects[key] = value
            else:
                configuration[key] = value

        # Copy configuration into each result if missing.
        subjects = {subject: [dict(**configuration, **result) for result in results] for subject, results in subjects.items()}

        return subjects

    async def resolve(self):
        r"""
        Perform this maintenance task, i.e., read the JSON files and repackage
        them into subject specific JSON files.

        EXAMPLES::

            >>> from pathlib import Path
            >>> from tempfile import TemporaryDirectory
            >>> import asyncio

            >>> with TemporaryDirectory() as tmpdir:
            ...     tmpdir = Path(tmpdir)
            ...     with open(tmpdir / "a.json", "w") as json: _ = json.write('{"subject": [{"result": true}]}')
            ...     with open(tmpdir / "b.json", "w") as json: _ = json.write('{"subject": [{"result": false}]}')
            ...     join = Join(jsons=[tmpdir / "a.json", tmpdir / "b.json"], outdir=tmpdir)
            ...     asyncio.run(join.resolve())
            ...     with open(tmpdir / "subject.json") as json: print(json.read())
            {
              "subject": [
                {
                  "result": true
                },
                {
                  "result": false
                }
              ]
            }
            
        """
        from collections import defaultdict

        subjects = defaultdict(lambda: [])

        for parsed in self._resolve_parsed_data():
            for subject, values in self._resolve_create_subjects(parsed).items():
                subjects[subject].extend(values)

        for subject, values in subjects.items():
            with open(self._outdir / f"{subject}.json", "w") as output:
                import json
                json.dump({subject: values}, output, indent=2)


__test__ = {
    # doctests of click do not run unless explicitly mentioned here due to the click decorator.
    "Join.click": Join.click.__doc__,
}
