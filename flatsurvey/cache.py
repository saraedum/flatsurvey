r"""
Access cached results from previous runs.

Currently, the only cache we support is Amazon's DynamoDB/S3. It would be
fairly trivial to change that and allow for other similar systems as well.

TODO: This is WIP and not really implemented yet.

"""
#*********************************************************************
#  This file is part of flatsurvey.
#
#        Copyright (C) 2020 Julian Rüth
#
#  Flatsurvey is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Flatsurvey is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with flatsurvey. If not, see <https://www.gnu.org/licenses/>.
#*********************************************************************

from pinject import copy_args_to_internal_fields

from flatsurvey.reporting import DynamoDB

class Cache:
    @copy_args_to_internal_fields
    def __init__(self, table=DynamoDB.DEFAULT_TABLE, region=DynamoDB.DEFAULT_REGION):
        self._table = DynamoDB.table(table=table, region=region)

    def result(self, surface, job, exact=False):
        key = job if isinstance(job, str) else type(job).__name__

        from boto3.dynamodb.conditions import Key
        rows = self.query(
            IndexName="surface-job",
            KeyConditionExpression=Key("surface-key").eq(str(surface)) & Key("job").eq(key))
        if exact:
            rows = [row for row in rows if Cache.restore(row['surface']) == surface]
        return CacheRows(rows=rows, job=job, surface=surface)

    @classmethod
    def restore(cls, column, region=DynamoDB.DEFAULT_REGION):
        if 'restored' not in column:
            from zlib import decompress
            from pickle import loads
            from io import BytesIO
            import boto3
            s3 = boto3.client("s3", region_name=region)
            _, __, bucket, key  = column['pickle'].split('/')
            with BytesIO() as buffer:
                s3.download_fileobj(bucket, key, buffer)
                buffer.seek(0)
                pickle = decompress(buffer.read())
                column['restored'] = loads(pickle)
        return column['restored']

    def _get(self, action, **kwargs):
        response = action(**kwargs)
        rows = response['Items']

        while response.get('LastEvaluatedKey'):
            response = action(**kwargs, ExclusiveStartKey=response['LastEvaluatedKey'])
            rows.extend(response['Items'])

        return rows

    def scan(self, **kwargs):
        return self._get(self._table.scan, **kwargs)

    def query(self, **kwargs):
        return self._get(self._table.query, **kwargs)


class CacheRows:
    def __init__(self, rows, job, surface=None):
        self._rows = rows
        self._job = job
        self._surface = surface

    def reduce(self):
        return self._job.reduce([row['result'] if 'result' in row else None for row in self._rows])
