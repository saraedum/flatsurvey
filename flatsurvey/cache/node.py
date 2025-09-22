r"""
Nodes in a tree of :class:`Results`.
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


from flatsurvey.cache.pickles import Pickles


class Node:
    r"""
    A row from the cache with some added lazy-loading.

    EXAMPLES::

        >>> from flatsurvey.cache import Cache
        >>> cache = Cache(pickles=None, report=None)
        >>> Node(1, cache=cache, kind=None)
        1

    """

    def __init__(self, value, cache, kind):
        self._value = value
        self._cache = cache
        self._kind = kind

    def __repr__(self):
        r"""
        Return a printable representation of this node, i.e., the underlying raw data.

        EXAMPLES::

            >>> from flatsurvey.cache import Cache
            >>> cache = Cache(pickles=None, report=None)
            >>> Node({}, cache=cache, kind=None)
            {}

        """
        return repr(self._value)

    def __getattr__(self, name):
        r"""
        Return the lazily resolved attribute ``name`` on this node.

        EXAMPLES::

            >>> from io import StringIO
            >>> from flatsurvey.cache import Cache
            >>> cache = Cache(jsons=[StringIO('{"surface": [{"type": "Ngon", "angles": [1, 2, 4], "pickle": "a1b54e02ade464584920abcbfd07faaa71afac1d5b455a56d5cf790ccf5528da"}]}')], pickles=None, report=None)
            >>> node = Node({"surface": "a1b54e02ade464584920abcbfd07faaa71afac1d5b455a56d5cf790ccf5528da"}, cache=cache, kind=None)
            >>> node.surface.type
            'Ngon'

        """
        try:
            for source in self._cache.sources():
                if source == "CACHE":
                    if isinstance(self._value, dict) and name in self._value:
                        return self._cache.make(self._value[name], name=name)

                if source == "PICKLE":
                    if isinstance(self._value, dict) and "pickle" in self._value:
                        # TODO: ignore "dropped", try to read the pickle directly, or read from the hash.pickle.gz, or continue with a warning.
                        instance = self._cache.unpickle(
                            self._value["pickle"], self._kind
                        )
                        return getattr(instance, name)

                if source == "DEFAULTS":
                    defaults = self._cache.defaults()
                    if name in defaults:
                        return defaults[name]

        except AttributeError as e:
            raise RuntimeError(e)

        raise AttributeError(f"{self} has no {name}")


class ReferenceNode(Node):
    r"""
    A node that references a node in another cache source.

    EXAMPLES::

        >>> from io import StringIO
        >>> from flatsurvey.cache import Cache
        >>> cache = Cache(jsons=[StringIO('{"surface": [{"type": "Ngon", "angles": [1, 2, 4], "pickle": "a1b54e02ade464584920abcbfd07faaa71afac1d5b455a56d5cf790ccf5528da"}]}')], pickles=None, report=None)
        >>> ReferenceNode('a1b54e02ade464584920abcbfd07faaa71afac1d5b455a56d5cf790ccf5528da', "surface", cache=cache)
        {'type': 'Ngon', 'angles': [1, 2, 4], 'pickle': 'a1b54e02ade464584920abcbfd07faaa71afac1d5b455a56d5cf790ccf5528da'}

    """

    def __init__(self, sha, section, cache):
        super().__init__(sha, cache, section)

    def _resolve(self):
        r"""
        Return the node this node resolves to.
        """
        return self._cache.get(self._kind, self._value)

    def __getattr__(self, name):
        if name == "pickle":
            return self._value

        resolved = self._resolve()
        try:
            return getattr(resolved, name)
        except AttributeError:
            return super().__getattr__(name)

    def __repr__(self):
        return repr(self._resolve())


# TODO: Rename this file.
class ResultSet:
    def __init__(self, rows, sources, quorum="unique"):
        self._rows = rows
        self._sources = sources
        self._quorum = quorum

    def __repr__(self):
        return f"{len(self._rows)} cached results"

    def __len__(self):
        return len(self._rows)

    def __getattr__(self, name):
        if not self._rows:
            raise AttributeError("no results found in cache")

        if self._quorum == "unique":
            return self._getattr_unique(name)
        else:
            raise NotImplementedError

    def _getattr_unique(self, name):
        values = self._getattrs(name)
        assert values, "_getattr_unique should not be callable when there are now results"

        value = values.pop()
        for other in values:
            if other != value:
                raise AttributeError(f"{name} is inconsistent in this cached result set, found {values}")

        return value

    def _getattrs(self, name):
        return [self._getattr(row, name) for row in self._rows]

    def _getattr(self, row, name, source=None):
        # TODO: This pattern is not pretty. Also is it really sane to inherit the defaults down?
        def as_result_set(result):
            if isinstance(result, dict):
                return ResultSet([result], sources=self._sources, quorum="unique")
            return result

        if source is None:
            for source in self._sources:
                try:
                    return self._getattr(row, name, source=source)
                except AttributeError:
                    pass

            raise AttributeError(f"cached result has no {name}")

        if source == "CACHE":
            try:
                return as_result_set(row[name])
            except KeyError:
                raise AttributeError(f"cached result has no {name}")

        if isinstance(source, dict):
            try:
                return as_result_set(source[name])
            except KeyError:
                raise AttributeError(f"cached result has no {name}")

        if isinstance(source, Pickles):
            try:
                pickle = row["pickle"]
            except KeyError:
                raise AttributeError(f"cached result has no {name}")

            kind = row.get("type", None)

            return as_result_set(source.unpickle(pickle, kind))

        raise NotImplementedError(source)

    def __hash__(self):
        raise TypeError

    def __eq__(self, other):
        raise NotImplementedError

    def __ne__(self, other):
        raise NotImplementedError

    def __bool__(self):
        return bool(self._rows)

    def latest(self):
        raise NotImplementedError

    def unique(self):
        raise NotImplementedError

    def any(self):
        raise NotImplementedError

    def filter(self, predicate):
        rows = [result._rows[0] for result in self if predicate(result)]
        return ResultSet(rows, sources=self._sources, quorum=self._quorum)

    def __iter__(self):
        for row in self._rows:
            yield ResultSet([row], sources=self._sources, quorum="unique")
