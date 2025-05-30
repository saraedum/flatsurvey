r"""
Deformations of surfaces

During an orbit closure search, it might be necessary to replace a given
surface with a deformation in the orbit closure to be able to determine the
full dimension of the orbit closure of the original surface.

EXAMPLES::

    >>> from flatsurvey.surfaces import Ngon, Deformation
    >>> from flatsurf.geometry.pyflatsurf_conversion import from_pyflatsurf
    >>> from flatsurf import GL2ROrbitClosure

    >>> S = Ngon((1, 1, 1))

    >>> O = GL2ROrbitClosure(S.surface())

    >>> delta = [O.V2(v, 0).vector for v in O.lift(O.tangent_space_basis()[0])]
    >>> deformation = from_pyflatsurf((O._surface + delta).surface())

    >>> T = Deformation(deformation, S)
    >>> T
    Deformation of Ngon([1, 1, 1])

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

from flatsurvey.restart import Restart
from flatsurvey.surfaces.surface import Surface


class Deformation(Surface):
    def __init__(self, deformed, old):
        super().__init__(old._eliminate_marked_points)
        self._deformed = deformed
        self._old = old

    def __repr__(self):
        return f"Deformation of {self._old}"

    @property
    def orbit_closure_dimension_upper_bound(self):
        r"""
        Return an upper bound for the dimension of the orbit closure.

        This is the same as the upper bound for the surface before deformation.

        EXAMPLES::

            >>> from flatsurvey.surfaces import Ngon, Deformation
            >>> from flatsurf.geometry.pyflatsurf_conversion import from_pyflatsurf
            >>> from flatsurf import GL2ROrbitClosure

            >>> S = Ngon((1, 1, 1))

            >>> O = GL2ROrbitClosure(S.surface())

            >>> delta = [O.V2(v, 0).vector for v in O.lift(O.tangent_space_basis()[0])]
            >>> deformation = from_pyflatsurf((O._surface + delta).surface())

            >>> T = Deformation(deformation, S)
            >>> T.orbit_closure_dimension_upper_bound
            2

        """
        return self._old.orbit_closure_dimension_upper_bound

    def _surface(self):
        r"""
        Return the underlying sage-flatsurf surface.

        EXAMPLES::

            >>> from flatsurvey.surfaces import Ngon, Deformation
            >>> from flatsurf.geometry.pyflatsurf_conversion import from_pyflatsurf
            >>> from flatsurf import GL2ROrbitClosure

            >>> S = Ngon((1, 1, 1))

            >>> O = GL2ROrbitClosure(S.surface())

            >>> delta = [O.V2(v, 0).vector for v in O.lift(O.tangent_space_basis()[0])]
            >>> deformation = from_pyflatsurf((O._surface + delta).surface())

            >>> T = Deformation(deformation, S)
            >>> T.surface()
            Translation Surface in H_1(0) built from 2 isosceles triangles

        """
        return self._deformed

    def __hash__(self):
        return hash((self._deformed, self._old))

    def __eq__(self, other):
        return (
            isinstance(other, Deformation)
            and self._deformed == other._deformed
            and self._old == other._old
        )

    def cache_predicate(self, exact, cache=None):
        return lambda result: False

    class Restart(Restart):
        def __init__(self, deformed, old):
            self._deformation = Deformation(deformed=deformed, old=old)

        def restart(self, bindings):
            # We keep the reporting so that any data is written to the log
            # files for the undeformed surface.
            from flatsurvey.reporting import Report
            report = bindings.get(Report)
            report = report.deform(self._deformation)

            bindings = bindings.clone()
            bindings.forget(Report)
            bindings.define(Report, report)
            bindings.forget(Surface)
            bindings.define(Surface, self._deformation)

            return bindings
