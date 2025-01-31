r"""
Track Interval Exchange Transformations that cannot be decided.

EXAMPLES::

    >>> from flatsurvey.test.cli import invoke
    >>> from flatsurvey.worker.__main__ import worker
    >>> invoke(worker, "undetermined-iet", "--help") # doctest: +NORMALIZE_WHITESPACE
    Usage: worker undetermined-iet [OPTIONS]
      Tracks undetermined Interval Exchange Transformations.
    Options:
      --limit INTEGER  Zorich induction steps to perform before giving up  [default:
                       256]
      --help           Show this message and exit.

"""
# *********************************************************************
#  This file is part of flatsurvey.
#
#        Copyright (C) 2021 Julian Rüth
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

import time

import click
import cppyy
import pyeantic
import pyexactreal
import pyintervalxt
from pinject import copy_args_to_internal_fields
from sage.all import cached_method

from flatsurvey.jobs.flow_decomposition import FlowDecompositions
from flatsurvey.pipeline import Consumer
from flatsurvey.pipeline.util import PartialBindingSpec
from flatsurvey.ui.group import GroupedCommand

# TODO: Make this iet serializable in pyintervalxt by simply saying dumps(iet.forget())
# i.e., when serializing an IET of unknown type (as is this one because
# (a) it comes from C++ and was not constructed in Python and (b) it
# has intervalxt::sample::Lengths and not intervalxt::cppyy::Lengths)
# be smart about registering the right types in cppyy. (If possible.) See #10.
# TODO: Expose something like this construction() in intervalxt. See #10.
cppyy.cppdef(
    r"""
#include <boost/type_erasure/any_cast.hpp>

template <typename T> std::tuple<std::vector<eantic::renf_elem_class>, std::vector<int> > construction(T& iet) {
    std::vector<eantic::renf_elem_class> lengths;
    std::vector<int> permutation;
    const auto top = iet.top();
    const auto bottom = iet.bottom();
    for (auto& label : top) {
        lengths.push_back(boost::type_erasure::any_cast<eantic::renf_elem_class>(iet.lengths()->forget().get(label)));
    }
    for (auto& label : bottom) {
        permutation.push_back(std::find(std::begin(top), std::end(top), label) - std::begin(top));
    }

    return std::make_tuple(lengths, permutation);
}

template <typename T> int degree(T& iet) {
    auto label = *std::begin(iet.top());
    auto length = boost::type_erasure::any_cast<eantic::renf_elem_class>(iet.lengths()->forget().get(label));
    return length.parent().degree();
}
"""
)


class UndeterminedIntervalExchangeTransformation(Consumer):
    r"""
    Tracks undetermined Interval Exchange Transformations.

    EXAMPLES::

        >>> from flatsurvey.surfaces import Ngon
        >>> from flatsurvey.reporting import Report
        >>> from flatsurvey.jobs import FlowDecompositions, SaddleConnectionOrientations, SaddleConnections, SaddleConnectionOrientations
        >>> from flatsurvey.cache import Cache
        >>> surface = Ngon((1, 1, 1))
        >>> connections = SaddleConnections(surface)
        >>> orientations = SaddleConnectionOrientations(connections)
        >>> flow_decompositions = FlowDecompositions(surface=surface, report=Report([]), saddle_connection_orientations=orientations)
        >>> UndeterminedIntervalExchangeTransformation(surface=surface, report=Report([]), flow_decompositions=flow_decompositions, saddle_connection_orientations=orientations, cache=Cache())
        undetermined-iet

    """
    DEFAULT_LIMIT = 256

    @copy_args_to_internal_fields
    def __init__(
        self,
        surface,
        report,
        flow_decompositions,
        saddle_connection_orientations,
        cache,
        limit=DEFAULT_LIMIT,
    ):
        super().__init__(producers=[flow_decompositions])

    @classmethod
    @click.command(
        name="undetermined-iet",
        cls=GroupedCommand,
        group="Goals",
        help=__doc__.split("EXAMPLES")[0],
    )
    @click.option(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        show_default=True,
        help="Zorich induction steps to perform before giving up",
    )
    def click(limit):
        return {
            "goals": [UndeterminedIntervalExchangeTransformation],
            "bindings": [
                PartialBindingSpec(UndeterminedIntervalExchangeTransformation)(
                    limit=limit
                )
            ],
        }

    def command(self):
        command = ["undetermined-iet"]
        if self._limit != UndeterminedIntervalExchangeTransformation.DEFAULT_LIMIT:
            command += ["--limit", str(self._limit)]
        return command

    async def _consume(self, decomposition, cost):
        r"""
        Track any undetermined IETs in this ``decomposition``.

        """
        for component in decomposition.decomposition.components():
            if component.withoutPeriodicTrajectory():
                continue
            if component.cylinder():
                continue

            iet = component.dynamicalComponent().iet()

            # Forget the surface structure of this IET
            construction = cppyy.gbl.construction(iet)
            degree = construction[0][0].parent().degree()
            iet = pyintervalxt.IntervalExchangeTransformation(
                list(construction[0]), list(construction[1])
            )

            start = time.perf_counter()
            induction = iet.induce(self._limit)
            cost += time.perf_counter() - start

            if str(induction) != "LIMIT_REACHED":
                continue

            assert not iet.boshernitzanNoPeriodicTrajectory()

            iet = pyintervalxt.IntervalExchangeTransformation(
                list(construction[0]), list(construction[1])
            )
            # TODO: pyintervalxt fails to serialize IETs. See #10.
            await self._report.result(
                self,
                str(iet),
                surface=self._surface,
                degree=degree,
                intervals=iet.size(),
                saf=list(iet.safInvariant()),
                orientation=self._saddle_connection_orientations._current,
            )

        return not Consumer.COMPLETED

    @classmethod
    def reduce(self, results):
        r"""
        Given a list of historic results, return a final verdict.

        """
        return None
