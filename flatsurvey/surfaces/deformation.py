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

import flatsurvey.worker.restart
from flatsurvey.surfaces.surface import Surface


class Deformation(Surface):
    def __init__(self, deformed, old):
        self._deformed = deformed
        self._old = old

    def __repr__(self):
        return f"Deformation of {self._old}"

    @property
    def _eliminate_marked_points(self):
        return self._old._eliminate_marked_points

    @property
    def orbit_closure_dimension_upper_bound(self):
        return self._old.orbit_closure_dimension_upper_bound

    def _surface(self):
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

    class Restart(flatsurvey.worker.restart.Restart):
        def __init__(self, deformed, old):
            self._deformation = Deformation(deformed=deformed, old=old)

        def restart(self, pipeline):
            # We keep the reporting so that any data is written to the log
            # files for the undeformed surface.
            from flatsurvey.reporting import Report
            report = pipeline.get(Report)

            pipeline = pipeline.clone()
            pipeline.forget(Report)
            pipeline.define(Report, report)
            pipeline.forget(Surface)
            pipeline.define(Surface, self._deformation)

            return pipeline
