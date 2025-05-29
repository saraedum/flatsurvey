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

from abc import ABC, abstractmethod

from flatsurvey.pipeline import Bindings


class Restart(Exception, ABC):
    r"""
    An exception that signals that the worker should restart on a modified
    bindings.
    """
    @abstractmethod
    def restart(self, bindings: Bindings) -> Bindings:
        r"""
        Return a modified bindings that the worker should work on instead after
        the restart.
        """
        raise NotImplementedError
