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

The fundamental object here is the :class:`Bindings`. It holds the rules to
produce the object graph to run a survey of surfaces or a survey on a single
surface. Basically, a bindings can hold for each type (or string key) a rule on
how to produce it::

    >>> from flatsurvey.pipeline import Bindings
    >>> bindings = Bindings()

The easiest rules are just constants::

    >>> class Surface():
    ...     def __repr__(self): return "surface"

    >>> bindings.define(Surface, Surface())

Whenever somebody needs a Surface, we answer with this constant::

    >>> bindings.get(Surface)
    surface

The binding values can also be types, as long as they have a static
``create`` method::

    >>> class SaddleConnections:
    ...     def __init__(self, surface):
    ...         self._surface = surface 
    ...
    ...     @staticmethod
    ...     def create(bindings): return SaddleConnections(bindings.get(Surface))

    >>> bindings.define("sc", SaddleConnections)

    >>> bindings.get("sc")._surface
    surface

Note that the constant is cached, you get the identical object every single
time::

    >>> bindings.get("sc") is bindings.get("sc")
    True

When asking for a type that has not been registered with ``define``, its
``create`` is also called automatically::

    >>> bindings.get(SaddleConnections)._surface
    surface

We cannot redefine names that have been requested already::

    >>> bindings.define(SaddleConnections, SaddleConnections("..."))
    Traceback (most recent call last):
    ...
    ValueError: cannot redefine ... in this bindings

However, we can explicitly "forget" values and bindings for a key::

    >>> bindings.forget(SaddleConnections)
    >>> bindings.define(SaddleConnections, SaddleConnections("..."))

    >>> bindings.get(SaddleConnections)._surface
    '...'

We can also only define or override a variable in a certain scope::

    >>> class OrbitClosure:
    ...     def __init__(self, sc, ambient):
    ...         self._sc = sc
    ...         self._ambient = ambient
    ...
    ...     @staticmethod
    ...     def create(bindings):
    ...         with bindings.scope(OrbitClosure) as scoped:
    ...             return OrbitClosure(scoped.get(SaddleConnections), scoped.get(str))
    ...     

    >>> with bindings.scope(OrbitClosure) as scoped:
    ...     scoped.define(str, "H_6(5^2, 0^2)")

    >>> bindings.get(OrbitClosure)._ambient
    'H_6(5^2, 0^2)'

Oftentimes, you want to incrementally register a list of things under one key,
say the goals of a survey::

    >>> bindings.append("Goals", OrbitClosure)
    >>> bindings.append("Goals", "something else")

    >>> bindings.get("Goals")
    [<flatsurvey.pipeline.bindings.OrbitClosure object at 0x...>, 'something else']

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
from typing import overload, Type, override, Protocol, cast, Iterator, Iterable

Key = str | Type


class HasCreate[T](Protocol):
    r"""
    A type that can be created from the bindings in the Bindings.
    """
    @staticmethod
    def create(bindings: "Bindings") -> T: ... 


class Binding[T](ABC):
    r"""
    A binding for a value stored in a Bindings.
    """
    @overload
    @staticmethod
    def create(value: "Binding[T]") -> "Binding[T]": ...

    @overload
    @staticmethod
    def create(value: T) -> "Binding[T]": ...

    @overload
    @staticmethod
    def create(value: Type[T]) -> "Binding[T]": ...

    @staticmethod
    def create(value):
        r"""
        Create a Binding from ``value``.

        EXAMPLES::

            >>> from flatsurvey.pipeline.bindings import Binding
            >>> Binding.create(123)
            ConstantBinding(123)

            >>> Binding.create(Binding)
            TypeBinding(Binding)

        """
        if isinstance(value, Binding):
            return value

        if isinstance(value, type):
            return TypeBinding(value)

        return ConstantBinding(value)

    @abstractmethod
    def resolve(self, bindings: "Bindings") -> T:
        r"""
        Return the value of this binding.

        Subclasses must implement this.
        """
        raise NotImplementedError

    @abstractmethod
    def clone(self) -> "Binding[T]":
        pass


class ConstantBinding[T](Binding[T]):
    r"""
    A constant value to be stored in a Bindings.

    EXAMPLES::

        >>> from flatsurvey.pipeline.bindings import Binding, Bindings
        >>> bindings = Bindings()

        >>> binding = Binding.create(123)

        >>> binding.resolve(bindings)
        123

    """
    def __init__(self, value: T):
        self._value = value

    @override
    def resolve(self, bindings: "Bindings") -> T:
        return self._value

    def __repr__(self):
        return f"ConstantBinding({self._value})"

    def clone(self):
        return ConstantBinding(self._value)


class TypeBinding[T : HasCreate](Binding[T]):
    r"""
    A value that is invoking ``.create`` on a type.

    EXAMPLES::

        >>> from flatsurvey.pipeline.bindings import Binding, Bindings
        >>> bindings = Bindings()


        >>> class A:
        ...     @staticmethod
        ...     def create(bindings): return A()

        >>> binding = Binding.create(A)

        >>> binding.resolve(bindings)
        <flatsurvey.pipeline.bindings.A object at 0x...>

    """
    def __init__(self, type: Type[T]):
        self._type = type

    @override
    def resolve(self, bindings) -> T:
        try:
            return self._type.create(bindings)
        except Exception as e:
            raise BindingException(f"Cannot create instance of '{self._type.__name__}' from bindings") from e

    def __repr__(self):
        return f"TypeBinding({self._type.__name__})"

    def clone(self):
        return TypeBinding(self._type)


class ListBinding[T](Binding[list[T]]):
    r"""
    A value that is an (expandable) list of other bindings.

    EXAMPLES::

        >>> from flatsurvey.pipeline.bindings import ListBinding, Bindings, Binding
        >>> bindings = Bindings()


        >>> binding = ListBinding()
        >>> binding.append(Binding.create(1))
        >>> binding.append(Binding.create(2))

        >>> binding.resolve(bindings)
        [1, 2]

    """
    def __init__(self):
        self._value: list[Binding[T]] = []

    def append(self, binding):
        self._value.append(binding)

    @override
    def resolve(self, bindings: "Bindings") -> list[T]:
        return [binding.resolve(bindings) for binding in self._value]

    def __repr__(self):
        return f"ListBinding({self._value})"

    def clone(self):
        clone = ListBinding()
        clone._value = self._value[:]
        return clone


class Bindings:
    r"""
    Rules to create the object graph performing a survey.

    EXAMPLES:

    Typically, a survey creates such a bindings to describe the general setup::

        >>> from flatsurvey.pipeline import Bindings
        >>> survey = Bindings()
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
    def __init__(self, repr=None):
        self._values = {}
        self._bindings = {}
        self._scopes = {}
        self._survey = {}
        self._repr: str | None = repr

    @staticmethod
    def click(wrapped):
        r"""
        Decorator helper to add a bindings argument to a click command handler.
        """
        from functools import wraps
        @wraps(wrapped)
        def command(*args, **kwargs):
            def wrapper(bindings: Bindings):
                wrapped(bindings, *args, **kwargs)
            return wrapper

        return command

    @contextmanager
    def scope(self, scope: str | Type):
        r"""
        Return the scoped bindings for ``scope``.
        """
        if scope not in self._scopes:
            self._scopes[scope] = Bindings()

        try:
            yield self._scopes[scope]
        except Exception as e:
            raise Exception(f"Error while in binding scope '{scope}'") from e

    def append(self, key: Key, value):
        r"""
        Append ``value`` to the list binding for ``key``.

        TODO: Register under list[key].
        """
        if key not in self._bindings:
            self._bindings[key] = ListBinding()

        self._bindings[key].append(Binding.create(value))

    def survey(self, key: Key, values: Iterable[object]):
        if key not in self._survey:
            self._survey[key] = []
        self._survey[key].append(values)

    @property
    def survey_bindings(self) -> Iterator["Bindings"]:
        from more_itertools import roundrobin
        sources = {key: roundrobin(*values) for key, values in self._survey.items()}
        from itertools import product

        for values in product(*sources.values()):
            keys = sources.keys()
            bindings = self.clone(repr=f"Bindings(survey {','.join(f'{key}={value}' for key, value in zip(keys, values))})")
            for key, value in zip(keys, values):
                bindings.define(key, value)

            yield bindings

    @property
    def potential_memory_leaks(self):
        # TODO: Try to find pyflatsurf objects and such somehow.
        return None

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
            if key in self._bindings:
                raise ValueError(f"cannot redefine {key} in this bindings");

            self._bindings[key] = Binding.create(value)

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

        The values must be actual values and not types or bindings.
        """
        if key is not None:
            if key in self._values:
                raise ValueError(f"cannot reset {key} in this bindings");

            self._values[key] = value

        for key, value in values.items():
            self.set(key=key, value=value)

    def get[T](self, key: Key, default: T | Callable[[], T] | None = None) -> T:
        r"""
        Resolve the ``key`` in this bindings.

        If no ``key`` has been registered in this scope or a parent scope,
        return ``default`` if set.

        TODO: This is not true. It's more complicated. (And it does not make too much sense.)
        """
        try:
            if key not in self._values:
                if key not in self._bindings:
                    if default is None:
                        if isinstance(key, type):
                            self.define(key=key, value=key)
                        else:
                            raise Exception(f"cannot resolve {key} in this bindings and no default given")
                    else:
                        if callable(default):
                            default = cast(T, default())

                        return default

                value = self._bindings[key].resolve(self)
                self.set(key, value)

            return self._values[key]
        except Exception as e:
            raise BindingException(f"Cannot resolve '{key}' from bindings") from e

    def describe(self, key: Key):
        if key in self._values:
            return repr(self._values[key])

        if key in self._bindings:
            return self._bindings[key]

        return "?"

    def __repr__(self):
        return self._repr or super().__repr__()

    def clone(self, repr=None):
        r"""
        Return a copy of the bindings.

        The copy has the same bindings but forgets about the concrete values
        that these bindings produced if any.

        EXAMPLES::

            >>> from flatsurvey.pipeline import Bindings
            >>> bindings = Bindings()

            >>> class Surface():
            ...     def __repr__(self): return "surface"
            ...     @staticmethod
            ...     def create(bindings): return Surface()

            >>> bindings.get(Surface) is bindings.get(Surface)
            True

            >>> clone = bindings.clone()
            >>> bindings.get(Surface) is clone.get(Surface)
            False

        """
        clone = Bindings(repr=repr)
        clone._bindings = {key: binding.clone() for key, binding in self._bindings.items()}
        clone._scopes = {scope: child.clone() for scope, child in self._scopes.items()}
        if clone._survey:
            raise NotImplementedError("cannot clone a survey binding")
        return clone

    def forget(self, key: Key):
        # TODO: Forget recursively in scopes. Should we?
        if key in self._bindings:
            del self._bindings[key]

        if key in self._values:
            del self._values[key]

        # TODO: Should we really forget scopes here?
        if key in self._scopes:
            del self._scopes[key]


class BindingException(Exception):
    pass
