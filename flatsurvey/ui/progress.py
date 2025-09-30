r"""
Progress tracking during surveys
"""
# *********************************************************************
#  This file is part of flatsurvey.
#
#        Copyright (C) 2025 Julian Rüth
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
from contextlib import ContextDecorator
from typing import Any

from alive_progress import alive_bar

class SurveyProgress(ContextDecorator):
    def queued(self):
        pass

    def completed(self):
        pass

    def set_activity(self, activity):
        del activity

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        del exc


r"""
A simple text based progress indicator that is visible while its context is
active.
"""
class StdoutSurveyProgress(SurveyProgress):
    def __init__(self, activity: str):
        self._bar = alive_bar(title=activity)
        self._context: Any = None
        self._queued = 0
        self._scheduled = 0

    def _update_text(self):
        r"""
        Make sure that the progress bar shows the correct number of pending
        tasks.
        """
        self._context.text(f"{self._queued} pending")

    def queued(self):
        r"""
        Register that another task was queued (it may or may not be executing
        already.)
        """
        self._queued += 1
        self._update_text()

    def completed(self):
        r"""
        Register that a previously queued task has run to completion.
        """
        self._queued -= 1
        self._update_text()
        self._context()

    def set_activity(self, activity):
        r"""
        Set the title of the progress indicator to ``activity``.
        """
        self._context.title(activity)

    def __enter__(self):
        r"""
        Display the progress indicator while the returned context is active.
        """
        self._context = self._bar.__enter__()
        self._update_text()
        return self

    def __exit__(self, *exc):
        r"""
        Hide the progress indicator.
        """
        self._context = None
        self._bar.__exit__(*exc)


class HiddenSurveyProgress(SurveyProgress):
    pass
