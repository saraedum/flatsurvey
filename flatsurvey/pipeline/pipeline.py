r"""
Utilities to create the graph of objects that are performing a survey.

.. NOTE::

    This is essentially a scaled down dependency-injection framework.
    Dependency-injection is not really a thing in the Python world. Initially,
    we were using pinject but it's discontinued and it was also even more
    confusing to work with so we rolled our own.

.. NOTE::

    The principal feature we want here from the dependency injection is that it
    lets us configure aspects of the object graph (say the way we search for
    saddle connections) or we can leave them unconfigured and just use the
    defaults.

EXAMPLES:

The fundamental object here is the :class:`Pipeline`. It holds the rules to
produce the object graph to run a survey of surfaces or a survey on a single
surface. Basically, a pipeline can hold for each type (or string key) a rule on
how to produce it::

    >>> from flatsurvey.pipeline import Pipeline
    >>> pipeline = Pipeline()

The easiest rules are just constants::

    >>> class Surface():
    ...     def __repr__(self): return "surface"

    >>> pipeline.define(Surface, Surface())

Whenever somebody needs a string, we answer with this constant::

    >>> pipeline.get(Surface)
    surface

The definition values can also be types, as long as they have a static
``create`` method::

    >>> class SaddleConnections:
    ...     def __init__(self, surface):
    ...         self._surface = surface 
    ...
    ...     @staticmethod
    ...     def create(pipeline): return SaddleConnections(pipeline.get(Surface))

    >>> pipeline.define("sc", SaddleConnections)

    >>> pipeline.get("sc")._surface
    surface

Note that the constant is cached, you get the identical object every single
time::

    >>> pipeline.get("sc") is pipeline.get("sc")
    True

When asking for a type that has not been registered with ``define``, its
``create`` is also called automatically::

    >>> pipeline.get(SaddleConnections)._surface
    surface

We cannot redefine names that have been requested already::

    >>> pipeline.define(SaddleConnections, SaddleConnections("..."))
    Traceback (most recent call last):
    ...
    ValueError: cannot redefine ... in this pipeline

However, we can explicitly "forget" values and definitions for a key::

    >>> pipeline.forget(SaddleConnections)
    >>> pipeline.define(SaddleConnections, SaddleConnections("..."))

    >>> pipeline.get(SaddleConnections)._surface
    '...'

We can also only define or override a variable in a certain scope::

    >>> class OrbitClosure:
    ...     def __init__(self, sc, ambient):
    ...         self._sc = sc
    ...         self._ambient = ambient
    ...
    ...     @staticmethod
    ...     def create(pipeline):
    ...         with pipeline.scope(OrbitClosure) as scoped:
    ...             return OrbitClosure(scoped.get(SaddleConnections), scoped.get(str))
    ...     

    >>> with pipeline.scope(OrbitClosure) as scoped:
    ...     scoped.define(str, "H_6(5^2, 0^2)")

    >>> pipeline.get(OrbitClosure)._ambient
    'H_6(5^2, 0^2)'

Oftentimes, you want to incrementally register a list of things under one key,
say the goals of a survey::

    >>> pipeline.append("Goals", OrbitClosure)
    >>> pipeline.append("Goals", "something else")

    >>> pipeline.get("Goals")
    [<flatsurvey.pipeline.pipeline.OrbitClosure object at 0x...>, 'something else']

"""
# *********************************************************************
#  This file is part of flatsurvey.
#
#        Copyright (C) 2024-2025 Julian Rüth
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
from collections.abc import Callable
from contextlib import contextmanager
from typing import overload, Type, override, Protocol, cast

Key = str | Type


class HasCreate[T](Protocol):
    r"""
    A type that can be created from the definitions in the Pipeline.
    """
    @staticmethod
    def create(pipeline: "Pipeline") -> T: ... 


class Definition[T](ABC):
    r"""
    A definition for a value stored in a Pipeline.
    """
    @overload
    @staticmethod
    def create(value: "Definition[T]") -> "Definition[T]": ...

    @overload
    @staticmethod
    def create(value: T) -> "Definition[T]": ...

    @overload
    @staticmethod
    def create(value: Type[T]) -> "Definition[T]": ...

    @staticmethod
    def create(value):
        r"""
        Create a Definition from ``value``.

        EXAMPLES::

            >>> from flatsurvey.pipeline.pipeline import Definition
            >>> Definition.create(123)
            ConstantDefinition(123)

            >>> Definition.create(Definition)
            TypeDefinition(Definition)

        """
        if isinstance(value, Definition):
            return value

        if isinstance(value, type):
            return TypeDefinition(value)

        return ConstantDefinition(value)

    @abstractmethod
    def resolve(self, pipeline: "Pipeline") -> T:
        r"""
        Return the value of this definition.

        Subclasses must implement this.
        """
        raise NotImplementedError


class ConstantDefinition[T](Definition[T]):
    r"""
    A constant value to be stored in a Pipeline.

    EXAMPLES::

        >>> from flatsurvey.pipeline.pipeline import Definition, Pipeline
        >>> pipeline = Pipeline()

        >>> definition = Definition.create(123)

        >>> definition.resolve(pipeline)
        123

    """
    def __init__(self, value: T):
        self._value = value

    @override
    def resolve(self, pipeline: "Pipeline") -> T:
        return self._value

    def __repr__(self):
        return f"ConstantDefinition({self._value})"


class TypeDefinition[T : HasCreate](Definition[T]):
    r"""
    A value that is invoking ``.create`` on a type.

    EXAMPLES::

        >>> from flatsurvey.pipeline.pipeline import Definition, Pipeline
        >>> pipeline = Pipeline()


        >>> class A:
        ...     @staticmethod
        ...     def create(pipeline): return A()

        >>> definition = Definition.create(A)

        >>> definition.resolve(pipeline)
        <flatsurvey.pipeline.pipeline.A object at 0x...>

    """
    def __init__(self, type: Type[T]):
        self._type = type

    @override
    def resolve(self, pipeline) -> T:
        return pipeline._values.get(self._type, self._type.create(pipeline))

    def __repr__(self):
        return f"TypeDefinition({self._type.__name__})"


class ListDefinition[T](Definition[list[T]]):
    r"""
    A value that is an (expandable) list of other definitions.

    EXAMPLES::

        >>> from flatsurvey.pipeline.pipeline import ListDefinition, Pipeline, Definition
        >>> pipeline = Pipeline()


        >>> definition = ListDefinition()
        >>> definition.append(Definition.create(1))
        >>> definition.append(Definition.create(2))

        >>> definition.resolve(pipeline)
        [1, 2]

    """
    def __init__(self):
        self._value: list[Definition[T]] = []

    def append(self, definition):
        self._value.append(definition)

    @override
    def resolve(self, pipeline: "Pipeline") -> list[T]:
        return [definition.resolve(pipeline) for definition in self._value]

    def __repr__(self):
        return f"ListDefinition({self._value})"


class Pipeline:
    r"""
    Rules to create the object graph performing a survey.

    EXAMPLES:

    Typically, a survey creates such a pipeline to describe the general setup::

        >>> from flatsurvey.pipeline import Pipeline
        >>> survey = Pipeline()
        >>> survey.append("goals", "some goal")
        >>> survey.define("surfaces", ["surface0", "surface1"])

    To perform the survey, the survey is iterating over the surfaces and
    sending a patched object graph to each worker::

        >>> surfaces = survey.get("surfaces")

        >>> work_template = survey.clone()
        >>> work_template.forget("surfaces")

        >>> for surface in surfaces:
        ...     work = work_template.clone()
        ...     work.define("surface", surface)
        ...     # would send item to an actual worker process and process it there
        ...     work.get("surface"), work.get("goals")
        ('surface0', ['some goal'])
        ('surface1', ['some goal'])

    """
    def __init__(self, parent: "Pipeline | None" = None):
        self._parent = parent
        self._values = {}
        self._definitions = {}
        self._scopes = {}

    @staticmethod
    def click(wrapped):
        r"""
        Decorator helper to add a pipeline argument to a click command handler.
        """
        def command(*args, **kwargs):
            def wrapper(pipeline: Pipeline):
                wrapped(pipeline, *args, **kwargs)
            return wrapper

        return command

    @contextmanager
    def scope(self, scope: str | Type):
        r"""
        Return the scoped pipeline for ``scope``.
        """
        if scope not in self._scopes:
            self._scopes[scope] = Pipeline(self)

        yield self._scopes[scope]

    def append(self, key: Key, value):
        r"""
        Append ``value`` to the list definition for ``key``.
        """
        if key not in self._definitions:
            self._definitions[key] = ListDefinition()

        self._definitions[key].append(Definition.create(value))

    @overload
    def define(self, key: Key, value: object): ...

    @overload
    def define(self, **value): ...

    def define(self, key: Key | None=None, value=None, **values):
        r"""
        Set the rule to create ``key`` to ``value``.

        Alternatively, key/value pairs can be given as keyword arguments.
        """
        if key is not None:
            if key in self._definitions:
                raise ValueError(f"cannot redefine {key} in this pipeline");

            self._definitions[key] = Definition.create(value)

        for key, value in values.items():
            self.define(key=key, value=value)

    @overload
    def set(self, key: Key, value: object): ...

    @overload
    def set(self, **value): ...

    def set(self, key: Key | None=None, value=None, **values):
        r"""
        Set the value of ``key`` to the actual ``value``.

        Alternatively, key/value pairs can be given as keyword arguments.

        For constant values, this is essentially like ``define``, however,
        ``forget`` will forget about the values set with ``set``.

        The values must be actual values and not types or definitions.
        """
        if key is not None:
            if key in self._values:
                raise ValueError(f"cannot reset {key} in this pipeline");

            self._values[key] = value

        for key, value in values.items():
            self.set(key=key, value=value)

    def get[T](self, key: Key, default: T | Callable[[], T] | None = None) -> T:
        r"""
        Resolve the ``key`` in this pipeline.

        If no ``key`` has been register in this scope or a parent scope, return
        ``default`` if set.
        """
        if key not in self._values:
            if key not in self._definitions:
                if self._parent:
                    return self._parent.get(key, default)

                if isinstance(key, type):
                    self.define(key=key, value=key)
                else:
                    if default is None:
                        raise Exception(f"cannot resolve {key} in this pipeline and no default given")

                    if callable(default):
                        return cast(T, default())

                    return default

            value = self._definitions[key].resolve(self)
            self.set(key, value)

        return self._values[key]

    def __repr__(self):
        return f"Pipeline with definitions {self._definitions} and values {self._values}"

    def clone(self):
        r"""
        Return a copy of the pipeline.

        The copy has the same definitions but forgets about the concrete values
        that these definitions produced if any.

        EXAMPLES::

            >>> from flatsurvey.pipeline import Pipeline
            >>> pipeline = Pipeline()

            >>> class Surface():
            ...     def __repr__(self): return "surface"
            ...     @staticmethod
            ...     def create(pipeline): return Surface()

            >>> pipeline.get(Surface) is pipeline.get(Surface)
            True

            >>> clone = pipeline.clone()
            >>> pipeline.get(Surface) is clone.get(Surface)
            False

        """
        clone = Pipeline()
        clone._definitions = dict(self._definitions)
        clone._scopes = {scope: child.clone() for (scope, child) in self._scopes.items()}
        return clone

    def forget(self, key: Key | None=None):
        if key is None:
            self._values = {}
            self._definitions = {}
            self._scopes = {}

        if key in self._definitions:
            del self._definitions[key]

        if key in self._values:
            del self._values[key]
