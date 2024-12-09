from contextlib import ContextDecorator

from alive_progress import alive_bar

class SurveyProgress(ContextDecorator):
    def __init__(self, activity):
        self._bar = alive_bar(title=activity)
        self._context = None
        self._queued = 0
        self._scheduled = 0

    def _update_text(self):
        self._context.text(f"{self._queued} pending")

    def queued(self):
        self._queued += 1
        self._update_text()

    def completed(self):
        self._queued -= 1
        self._update_text()
        self._context()

    def set_activity(self, activity):
        self._context.title(activity)

    def __enter__(self):
        self._context = self._bar.__enter__()
        self._update_text()
        return self

    def __exit__(self, *exc):
        self._context = None
        self._bar.__exit__(*exc)
