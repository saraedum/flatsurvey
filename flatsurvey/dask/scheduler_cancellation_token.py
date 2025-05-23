import dask.distributed

from flatsurvey.dask.worker_cancellation_token import WorkerCancellationToken, NORMAL, CANCEL, ABORT


class SchedulerCancellationToken:
    def __init__(self, client: dask.distributed.Client):
        self._client = client
        self._state = NORMAL

        import uuid
        self.id = str(uuid.uuid4())

    @property
    def cancelled(self):
        return self._state >= CANCEL

    def cancel(self):
        self._state = CANCEL

        async def cancel():
            await self._client.set_metadata(self.id, self._state)
            await self._client.run(WorkerCancellationToken.cancel, self.id)

        import asyncio
        asyncio.get_running_loop().create_task(cancel())

    def abort(self):
        self._state = ABORT

        async def abort():
            await self._client.set_metadata(self.id, self._state)
            await self._client.run(WorkerCancellationToken.abort, self.id)
            await self._client.close(0)  # pyright: ignore

        import asyncio
        asyncio.get_running_loop().create_task(abort())
