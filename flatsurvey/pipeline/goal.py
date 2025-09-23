r"""
A generic goal of a program run.

Invocations of flatsurvey run until all registered goals have been resolved.

EXAMPLES:

    >>> from flatsurvey.jobs import OrbitClosure
    >>> Goal in OrbitClosure.mro()
    True

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

from abc import ABC, abstractmethod


class Goal(ABC):
    @abstractmethod
    async def resolve(self):
        pass

