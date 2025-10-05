r"""
Computes the GL₂(R) orbit closure of a surface.

Naturally, this will only find a lower bound for the orbit closure, i.e., the
space it finds might be too small because not enough directions have been
investigated that would lead us to the full space.

EXAMPLES::

    >>> from flatsurvey.test.cli import invoke
    >>> from flatsurvey.worker import worker
    >>> invoke(worker, "orbit-closure", "--help") # doctest: +NORMALIZE_WHITESPACE
    Usage: worker orbit-closure [OPTIONS]
      Determines the GL₂(R) orbit closure of ``surface``.
    Options:
      --stale-limit INTEGER       abort search after processing that many flow
                                  decompositions with cylinders without an increase
                                  in dimension  [default: 32]
      --expansions-limit INTEGER  when the --stale-limit has been reached, restart
                                  the search with random saddle connections that are
                                  twice as long as the ones used previously; repeat
                                  this doubling process EXPANSIONS many times
                                  [default: 4]
      --deform / --no-deform      When set, we deform the input surface as soon as
                                  we found a third dimension in the tangent space
                                  and restart. This is often beneficial if the input
                                  surface has lots of symmetries and also when the
                                  Boshernitzan criterion can rarely be applied due
                                  to SAF=0.
      --cache-only                Do not perform any computation. Only query the
                                  cache.
      --help                      Show this message and exit.

Verify that this goal works in a non-survey run::

    >>> invoke(worker, "ngon", "-a", "1", "-a", "2", "-a", "4", "orbit-closure")  # doctest: +ELLIPSIS
    [OrbitClosure] ...
    [OrbitClosure] GL(2,R)-orbit closure of dimension at least 7 in H_3(3, 1) (ambient dimension 7) (dimension: 7) (directions: 8) (directions_with_cylinders: 8) (dense: True)

TESTS:

Verify that this goal works in a tiny survey run::

    >>> from pathlib import Path
    >>> from flatsurvey.survey import survey
    >>> from flatsurvey.reporting import Json
    >>> from tempfile import TemporaryDirectory

    >>> with TemporaryDirectory() as tmpdir:
    ...     tmpdir = Path(tmpdir)
    ...     invoke(survey, "--debug", "--quiet", "ngons", "--count", "2", "--vertices", "3", "orbit-closure", "json", "--prefix", tmpdir)
    ...     cache = Cache(Cache.load([tmpdir / "ngon-1-2-4.json", tmpdir / "ngon-2-2-3.json"]))

Validate the results of the "survey"::

    >>> cached = cache.get("orbit-closure")
    >>> len(cached)
    2
    >>> cached.dense
    True

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

from sage.misc.cachefunc import cached_method

from flatsurvey.ui import Command
from flatsurvey.pipeline import Consumer, Bindings, Goal
from flatsurvey.ui.group import GroupedCommand
from flatsurvey.cache import Cache
from flatsurvey.surfaces import Surface, Deformation
from flatsurvey.reporting import Report
from flatsurvey.jobs.flow_decompositions import FlowDecompositions
from flatsurvey.jobs.saddle_connections import SaddleConnections


class OrbitClosure(Consumer, Command):
    r"""
    Determines the GL₂(R) orbit closure of ``surface``.

    EXAMPLES::

        >>> from flatsurvey.surfaces import Ngon
        >>> from flatsurvey.jobs import FlowDecompositions, SaddleConnectionOrientations, SaddleConnections
        >>> surface = Ngon((1, 1, 1))
        >>> connections = SaddleConnections(surface)
        >>> flow_decompositions = FlowDecompositions(surface=surface, saddle_connection_orientations=SaddleConnectionOrientations(connections))
        >>> OrbitClosure(surface=surface, flow_decompositions=flow_decompositions, saddle_connections=connections, cache=None)
        orbit-closure

    """

    DEFAULT_STALE_LIMIT = 32
    DEFAULT_EXPANSIONS_LIMIT = 4
    DEFAULT_DEFORM = False

    def __init__(
        self,
        surface: Surface,
        flow_decompositions: FlowDecompositions,
        saddle_connections: SaddleConnections,
        cache: Cache,
        stale_limit=DEFAULT_STALE_LIMIT,
        expansions_limit=DEFAULT_EXPANSIONS_LIMIT,
        deform=DEFAULT_DEFORM,
        cache_only=Consumer.DEFAULT_CACHE_ONLY,
        report: Report | None = None,
    ):
        super().__init__(
            producers=[flow_decompositions],
            report=report,
            cache=cache,
            cache_only=cache_only,
        )

        self._surface = surface
        self._saddle_connections = saddle_connections
        self._stale_limit = stale_limit
        self._expansions_limit = expansions_limit
        self._cache_only = cache_only
        self._deform = deform

        from flatsurvey.surfaces.deformation import Deformation

        if isinstance(self._surface, Deformation):
            self._deform = False

        self._cylinders_without_increase = 0
        self._directions_with_cylinders = 0
        self._directions = 0
        self._expansions_performed = 0

        import pyflatsurf

        del pyflatsurf

        self._lower_bound = 0
        self._upper_bound = 0

    async def consume_cache(self):
        r"""
        Try to resolve this goal from cached previous runs.

        EXAMPLES::

            >>> from flatsurvey.surfaces import Ngon, Surface
            >>> from flatsurvey.jobs import FlowDecompositions, SaddleConnectionOrientations, SaddleConnections
            >>> from flatsurvey.cache import Cache
            >>> surface = Ngon((1, 1, 1))
            >>> connections = SaddleConnections(surface)
            >>> flow_decompositions = FlowDecompositions(surface=surface, saddle_connection_orientations=SaddleConnectionOrientations(connections))

        Try to resolve the goal from (no) cached results::

            >>> import asyncio
            >>> goal = OrbitClosure(surface=surface, flow_decompositions=flow_decompositions, saddle_connections=connections, cache=None)
            >>> asyncio.run(goal.consume_cache())

            >>> goal.resolved
            False

        We mock some artificial results from previous runs and consume that
        artificial cache::

            >>> from io import StringIO
            >>> cache = Cache({
            ...     "orbit-closure": [{
            ...         "surface": {
            ...             "type": "Ngon",
            ...             "angles": [1, 1, 1],
            ...         },
            ...         "dense": None,
            ...     }, {
            ...         "surface": {
            ...             "type": "Ngon",
            ...             "angles": [1, 1, 1],
            ...         },
            ...         "dense": True,
            ... }]})

            >>> goal = OrbitClosure(surface=surface, flow_decompositions=flow_decompositions, saddle_connections=connections, cache=cache)
            >>> asyncio.run(goal.consume_cache())

            >>> goal.resolved
            True

        TESTS:

        Check that JSON output for this goal works::

            >>> from flatsurvey.reporting import Json, Report

            >>> report = Report([Json({Surface: surface}, output="-")])
            >>> goal = OrbitClosure(surface=surface, report=report, flow_decompositions=flow_decompositions, saddle_connections=connections, cache=cache)

            >>> import asyncio
            >>> asyncio.run(goal.consume_cache())
            >>> report.flush()  # doctest: +ELLIPSIS
            {"surface": {"angles": [1, 1, 1], "type": "Ngon", "repr": "Ngon([1, 1, 1])"}, "orbit-closure": [{"timestamp": "...", "dense": true, "cached": true, "value": null}]}


        """
        with self._cache.defaults({"dense": None}):
            results = self._cache.get(OrbitClosure).filter(
                self._surface.cache_predicate(False, cache=self._cache)
            )

            verdict = None
            if results.any(lambda result: result.dense == True):
                verdict = True

        if verdict is not None or self._cache_only:
            await self._report.result(self, result=None, dense=verdict, cached=True)
            self._resolved = True

    @staticmethod
    def create(bindings: Bindings):
        r"""
        Return an ``OrbitClosure`` instance from the configuration
        registered in ``bindings``.

        TESTS::

            >>> from flatsurvey.pipeline import Bindings
            >>> from flatsurvey.test.cli import invoke_subcommand
            >>> from flatsurvey.surfaces.ngons import Ngon
            >>> bindings = Bindings()
            >>> invoke_subcommand(OrbitClosure.click, bindings=bindings)
            >>> invoke_subcommand(Ngon.click, "-a", "1", "-a", "1", "-a", "1", bindings=bindings)
            >>> OrbitClosure.create(bindings)
            orbit-closure

        """
        with bindings.scope(OrbitClosure) as scoped:
            return OrbitClosure(
                surface=bindings.get(Surface),
                report=bindings.get(Report),
                flow_decompositions=bindings.get(FlowDecompositions),
                saddle_connections=bindings.get(SaddleConnections),
                cache=bindings.get(Cache),
                stale_limit=scoped.get(
                    "stale_limit", lambda: OrbitClosure.DEFAULT_STALE_LIMIT
                ),
                expansions_limit=scoped.get(
                    "expansions_limit", lambda: OrbitClosure.DEFAULT_EXPANSIONS_LIMIT
                ),
                deform=scoped.get("deform", lambda: OrbitClosure.DEFAULT_DEFORM),
                cache_only=scoped.get(
                    "cache_only", lambda: Consumer.DEFAULT_CACHE_ONLY
                ),
            )

    @staticmethod
    @click.command(
        name="orbit-closure",
        cls=GroupedCommand,
        group="Goals",
        help=__doc__.split("EXAMPLES")[0],  # type: ignore
    )
    @click.option(
        "--stale-limit",
        type=int,
        default=DEFAULT_STALE_LIMIT,
        show_default=True,
        help="abort search after processing that many flow decompositions with cylinders without an increase in dimension",
    )
    @click.option(
        "--expansions-limit",
        type=int,
        default=DEFAULT_EXPANSIONS_LIMIT,
        show_default=True,
        help="when the --stale-limit has been reached, restart the search with random saddle connections that are twice as long as the ones used previously; repeat this doubling process EXPANSIONS many times",
    )
    @click.option(
        "--deform/--no-deform",
        default=DEFAULT_DEFORM,
        help="When set, we deform the input surface as soon as we found a third dimension in the tangent space and restart. This is often beneficial if the input surface has lots of symmetries and also when the Boshernitzan criterion can rarely be applied due to SAF=0.",
    )
    @Consumer._cache_only_option
    @Bindings.click
    def click(bindings: Bindings, stale_limit, expansions_limit, deform, cache_only):
        r"""
        Parse command line options into ``bindings``.

        TESTS::

            >>> from flatsurvey.test.cli import invoke_subcommand
            >>> invoke_subcommand(OrbitClosure.click)

        """
        bindings.append(list[Goal], OrbitClosure)
        with bindings.scope(OrbitClosure) as scoped:
            scoped.define(
                stale_limit=stale_limit,
                expansions_limit=expansions_limit,
                deform=deform,
                cache_only=cache_only,
            )

    @property
    def dimension(self):
        r"""
        Return the currently determined lower bound for the dimension of the orbit closure.

        EXAMPLES::

            >>> from flatsurvey.pipeline import Bindings
            >>> from flatsurvey.test.cli import invoke_subcommand
            >>> from flatsurvey.surfaces.ngons import Ngon
            >>> bindings = Bindings()
            >>> invoke_subcommand(OrbitClosure.click, bindings=bindings)
            >>> invoke_subcommand(Ngon.click, "-a", "1", "-a", "1", "-a", "1", bindings=bindings)
            >>> oc = OrbitClosure.create(bindings)

            >>> oc.dimension
            2

        """
        return self._orbit_closure().dimension()

    @cached_method
    def _orbit_closure(self):
        r"""
        Return the orbit closure of the surface (as has been determined so far.)

        EXAMPLES::

            >>> from flatsurvey.pipeline import Bindings
            >>> from flatsurvey.test.cli import invoke_subcommand
            >>> from flatsurvey.surfaces.ngons import Ngon
            >>> bindings = Bindings()
            >>> invoke_subcommand(OrbitClosure.click, bindings=bindings)
            >>> invoke_subcommand(Ngon.click, "-a", "1", "-a", "1", "-a", "1", bindings=bindings)
            >>> oc = OrbitClosure.create(bindings)

            >>> oc._orbit_closure()
            GL(2,R)-orbit closure of dimension at least 2 in H_1(0) (ambient dimension 2)

        """
        from flatsurf import GL2ROrbitClosure

        return GL2ROrbitClosure(self._surface.surface())

    @property
    def dense(self):
        r"""
        Return whether the orbit closure has already been determined to be dense.

        Returns ``None`` when there is no verdict yet.

        EXAMPLES::

            >>> from flatsurvey.pipeline import Bindings
            >>> from flatsurvey.test.cli import invoke_subcommand
            >>> from flatsurvey.surfaces.ngons import Ngon
            >>> bindings = Bindings()
            >>> invoke_subcommand(OrbitClosure.click, bindings=bindings)
            >>> invoke_subcommand(Ngon.click, "-a", "1", "-a", "1", "-a", "1", bindings=bindings)
            >>> oc = OrbitClosure.create(bindings)

            >>> oc.dense
            True

        """
        if self.dimension == self._surface.orbit_closure_dimension_upper_bound:
            return True

        return None

    async def _consume(self, product, cost):
        r"""
        Enlarge the orbit closure from the cylinders in the decomposition ``product``.

        EXAMPLES::

            >>> from flatsurvey.surfaces import Ngon, Surface
            >>> from flatsurvey.reporting import Log, Report
            >>> from flatsurvey.jobs import FlowDecompositions, SaddleConnectionOrientations, SaddleConnections
            >>> surface = Ngon((1, 3, 5))
            >>> connections = SaddleConnections(surface)
            >>> log = Log({Surface: surface}, output="-")
            >>> flow_decompositions = FlowDecompositions(surface=surface, saddle_connection_orientations=SaddleConnectionOrientations(connections))
            >>> oc = OrbitClosure(surface=surface, report=Report([log]), flow_decompositions=flow_decompositions, saddle_connections=connections, cache=None)

        Run until we find the orbit closure, i.e., investigate in two directions::

            >>> import asyncio
            >>> resolve = oc.resolve()
            >>> assert asyncio.run(resolve)
            [Ngon([1, 3, 5])] [OrbitClosure] dimension: 4/6
            [Ngon([1, 3, 5])] [OrbitClosure] dimension: 6/6
            [Ngon([1, 3, 5])] [OrbitClosure] GL(2,R)-orbit closure of dimension at least 6 in H_3(4) (ambient dimension 6) (dimension: 6) (directions: 2) (directions_with_cylinders: 2) (dense: True)

        TESTS:

        Check that the JSON output for this goal works::

            >>> from flatsurvey.reporting import Json

            >>> report = Report([Json({Surface: surface}, output="-")])
            >>> flow_decompositions = FlowDecompositions(surface=surface, saddle_connection_orientations=SaddleConnectionOrientations(connections))
            >>> oc = OrbitClosure(surface=surface, report=report, flow_decompositions=flow_decompositions, saddle_connections=connections, cache=None)

            >>> import asyncio
            >>> resolve = oc.resolve()
            >>> assert asyncio.run(resolve)

            >>> asyncio.run(oc.report())
            >>> report.flush()  # doctest: +ELLIPSIS
            {"surface": {"angles": [1, 3, 5], "type": "Ngon", "repr": "Ngon([1, 3, 5])"}, "orbit-closure": [{"timestamp": "...", "dimension": 6, "directions": 2, "directions_with_cylinders": 2, "dense": true, "value": {"type": "GL2ROrbitClosure", "repr": "GL(2,R)-orbit closure of dimension at least 6 in H_3(4) (ambient dimension 6)"}}]}


        """
        del cost

        self._directions += 1

        import pyflatsurf

        self._upper_bound = max(
            pyflatsurf.flatsurf.Bound.upper(self._saddle_connections._current.vector()),
            self._upper_bound,
        )

        if product.cylinders() and not product.undeterminedComponents():
            self._cylinders_without_increase += 1
            self._directions_with_cylinders += 1

        orbit_closure = self._orbit_closure()
        dimension = self.dimension

        # TODO: If this is a billiard, we should use symmetries, see https://github.com/flatsurf/sage-flatsurf/issues/35.
        orbit_closure.update_tangent_space_from_flow_decomposition(product)

        self._report.progress(
            source=self,
            what="dimension",
            count=self.dimension,
            total=self._surface.orbit_closure_dimension_upper_bound,
        )

        assert (
            self.dimension <= self._surface.orbit_closure_dimension_upper_bound
        ), "%s <= %s" % (
            self.dimension,
            self._surface.orbit_closure_dimension_upper_bound,
        )

        if dimension != self.dimension:
            self._cylinders_without_increase = 0

        if self.dimension == self._surface.orbit_closure_dimension_upper_bound:
            await self.report()
            # Dense orbit closure. Stop consuming further cylinder decompositions.
            return "COMPLETED"

        # TODO: This heuristics do not make a ton of sense. We should add
        # better logging to explain that this is a sane strategy. (Here and
        # also in the deformation below.)
        if self._cylinders_without_increase >= self._stale_limit:
            if self._expansions_performed < self._expansions_limit:
                self._expansions_performed += 1

                self._report.log(
                    self,
                    f"Found {self._cylinders_without_increase} cylinders without improvements. Let's try something else.",
                )

                if self._lower_bound == 0:
                    self._lower_bound = self._upper_bound
                else:
                    self._lower_bound *= 4

                if self._upper_bound > self._lower_bound:
                    self._report.log(
                        self,
                        "Continuing search since connections seem to be increasing in length quickly.",
                    )
                else:
                    self._saddle_connections.randomize(self._lower_bound)
                    self._report.log(
                        self,
                        f"Now considering directions coming from saddle connections of length more than {self._lower_bound}",
                    )
                self._cylinders_without_increase = 0
                return "NOT_COMPLETED"

            return "COMPLETED"

        if (
            self._deform
            and self.dimension > 3
            and self._directions >= self._stale_limit
        ):
            self._report.log(
                self,
                f"Explored {self._directions} directions with conclusion. Deforming surface.",
            )

            tangents = [
                orbit_closure.lift(v) for v in orbit_closure.tangent_space_basis()[2:]
            ]

            def upper_bound(v):
                length = sum(abs(x.parent().number_field(x)) for x in v) / len(v)

                n = 1
                while n < length:
                    n *= 2
                return n

            def height(v):
                bound = upper_bound(v)

                return max(
                    c.height()
                    for x in v
                    for c in (x.parent().number_field(x) / bound).list()
                )

            tangents.sort(key=height)

            # TODO: Try harder to find a deformation vector here of small
            # height here. Then scale it to be much shorter than things in the
            # surface. The coefficient explosion does not come from the 1/n
            # scaling but from the original deformation vector currently.
            scale = 1
            while True:
                eligibles = False

                for tangent in tangents:
                    import cppyy

                    # What is a good vector to use to deform? See #3.
                    n = upper_bound(tangent) * scale

                    eligibles = True

                    deformation = [
                        orbit_closure.V2(x / n, x / (2 * n)).vector for x in tangent
                    ]
                    try:
                        # Valid deformations that require lots of flips take forever. It's crucial to pick n such that no/very few flips are sufficient. See #3.
                        deformed = orbit_closure._surface + deformation

                        self._report.log(
                            self,
                            f"Deformed surface with {1/n} * tangent vector {tangent}.",
                        )

                        surface = deformed.surface()
                        from flatsurf.geometry.pyflatsurf_conversion import (
                            from_pyflatsurf,
                        )

                        surface = from_pyflatsurf(surface)

                        self._deformed = True

                        self._report.log(
                            self,
                            "Restarting OrbitClosure search with deformed surface.",
                        )

                        def create_bindings(old: Bindings):
                            deformation = OrbitClosureDeformation(
                                surface, old=self._surface
                            )

                            bindings = old.clone()
                            bindings.forget(Surface)
                            bindings.define(Surface, deformation)

                            return bindings

                        from flatsurvey.restart import Restart

                        # TODO: Explicitly test this code path in the doctests here
                        raise Restart(create_bindings)
                    except cppyy.gbl.std.invalid_argument:
                        self._report.log(
                            source=self,
                            message=f"Failed to deform {orbit_closure._surface} with {n}",
                        )
                        continue

                scale += 1

                if not eligibles:
                    self._report.progress(
                        source=self, message="failed to deform surface"
                    )

                    import logging

                    logging.error(
                        "Cannot deform. No tangent vector can be used to deform."
                    )
                    break

        return "NOT_COMPLETED"

    async def report(self, **kwargs):
        r"""
        Report our final verdict about this orbit closure.

        EXAMPLES::

            >>> from flatsurvey.surfaces import Ngon, Surface
            >>> from flatsurvey.reporting import Report, Json
            >>> from flatsurvey.jobs import FlowDecompositions, SaddleConnectionOrientations, SaddleConnections
            >>> surface = Ngon((1, 3, 5))
            >>> connections = SaddleConnections(surface)
            >>> report = Report([Json({Surface: surface}, output="-")])
            >>> flow_decompositions = FlowDecompositions(surface=surface, saddle_connection_orientations=SaddleConnectionOrientations(connections))
            >>> oc = OrbitClosure(surface=surface, report=report, flow_decompositions=flow_decompositions, saddle_connections=connections, cache=None)

            >>> import asyncio
            >>> asyncio.run(oc.report())

            >>> report.flush()
            {"surface": {...}, "orbit-closure": [{"timestamp": "...", "dimension": 2, ..., "dense": null, ...}}]}

        """
        if not self.reported():
            await self._report.result(
                self,
                self._orbit_closure(),
                dimension=self.dimension,
                directions=self._directions,
                directions_with_cylinders=self._directions_with_cylinders,
                dense=self.dense,
                **kwargs,
            )


class OrbitClosureDeformation(Deformation):
    @property
    def orbit_closure_dimension_upper_bound(self):
        r"""
        Return an upper bound for the dimension of the orbit closure.

        This is the same as the upper bound for the surface before deformation.

        EXAMPLES::

            >>> from flatsurvey.surfaces import Ngon
            >>> from flatsurf.geometry.pyflatsurf_conversion import from_pyflatsurf
            >>> from flatsurf import GL2ROrbitClosure

            >>> S = Ngon((1, 1, 1))

            >>> O = GL2ROrbitClosure(S.surface())

            >>> delta = [O.V2(v, 0).vector for v in O.lift(O.tangent_space_basis()[0])]
            >>> deformation = from_pyflatsurf((O._surface + delta).surface())

            >>> T = OrbitClosureDeformation(deformation, S)
            >>> T.orbit_closure_dimension_upper_bound
            2

        """
        return self._old.orbit_closure_dimension_upper_bound


__test__ = {
    # Work around https://trac.sagemath.org/ticket/33951
    "OrbitClosure._orbit_closure": OrbitClosure._orbit_closure.__doc__,
    "OrbitClosure.click": OrbitClosure.click.__doc__,
}
