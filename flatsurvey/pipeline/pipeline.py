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


class Definition:
    @staticmethod
    def create(value):
        if isinstance(value, type):
            value = TypeDefinition(value)

        if not isinstance(value, Definition):
            value = ConstantDefinition(value)

        return value


class ConstantDefinition(Definition):
    def __init__(self, value):
        self._value = value

    def resolve(self, pipeline: "Pipeline"):
        return self._value


class TypeDefinition(Definition):
    def __init__(self, type):
        self._type = type

    def resolve(self, pipeline):
        return self._type.create(pipeline)


class ListDefinition(Definition):
    def __init__(self):
        self._value = []

    def append(self, definition):
        self._value.append(definition)

    def resolve(self, pipeline: "Pipeline"):
        return [definition.resolve(pipeline) for definition in self._value]


class Pipeline:
    def __init__(self):
        self._values = {}
        self._definitions = {}

    @staticmethod
    def click(wrapped):
        def command(*args, **kwargs):
            def wrapper(pipeline: Pipeline):
                wrapped(pipeline, *args, **kwargs)
            return wrapper

        return command

    def append(self, key, value, scope=None):
        if scope is not None:
            key = (scope, key)

        if key not in self._definitions:
            self._definitions[key] = ListDefinition()

        self._definitions[key].append(Definition.create(value))

    def define(self, key=None, value=None, scope=None, **values):
        if key is not None:
            assert key not in values
            values[key] = value

        for key, value in values.items():
            if scope is not None:
                key = (scope, key)

            if key in self._definitions:
                raise ValueError(f"cannot redefine {key} in this pipeline");

            self._definitions[key] = Definition.create(value)

    def get(self, key, default=None, *, scope=None):
        if scope is not None:
            key = (scope, key)

        if key not in self._values:
            if key not in self._definitions:
                if isinstance(key, type):
                    self.define(key=key, value=key)
                else:
                    if default is not None:
                        return default()

                    raise Exception(f"cannot resolve {key} in this pipeline and no default given")

            value = self._definitions[key].resolve(self)
            self._values[key] = value

        return self._values[key]

    # def append(self, key, gettable, scope=None):
    #     key = (scope, key)

    #     self._bindings.setdefault(key, [])
    #     
    #     bound = self._bindings[key]
    #     if not isinstance(bound, list):
    #         raise NotImplementedError(f"cannot append to nonlist for {key}")

    #     bound.append(gettable)

    # def bind(self, key, gettable, scope=None):
    #     key = (scope, key)

    #     if key in self._bindings:
    #         raise ValueError("bindings cannot be modified")

    #     self._bindings[key] = gettable

    def describe(self, key):
        # TODO
        return str(self._definitions[key])

    def __repr__(self):
        return f"Pipeline with definitions {self._definitions} and values {self._values}"

    # def get(self, key, default=None, scope=None):
    #     if (scope, key) not in self._values:
    #         if (scope, key) in self._bindings:
    #             self.set(key, 
    #         if key not in self._bindings:
    #             if isinstance(key, type):
    #                 self._bindings[key] = (key, [], {})

    #         self.set(key, self.instantiate(self._bindings[key]))

    #     return self._values[key]

    # def instantiate(self, constructor):
    #     if isinstance(constructor, list):
    #         return [self.get(key) for key in constructor]

    #     if isinstance(constructor, type):
    #         constructor = (constructor, [], {})

    #     if isinstance(constructor, tuple) and len(constructor) == 3:
    #         constructor, args, kwargs = constructor
    #         from inspect import signature, Parameter

    #         parameters = list(signature(constructor).parameters.values())

    #         for param in parameters[len(args):]:
    #             name = param.name
    #             if name not in kwargs:
    #                 default = param.default
    #                 if default is not Parameter.empty:
    #                     kwargs[name] = param.default
    #                     continue

    #                 t = param.annotation
    #                 if t is not Parameter.empty:
    #                     kwargs[name] = self.get(t)
    #                     continue

    #                 if name in self._bindings or name in self._values:
    #                     kwargs[name] = self.get(name)
    #                     continue

    #                 raise ValueError(f"cannot instantiate {constructor} because {name} is missing a type annotation or default value and the name has no value or binding set in this pipeline")

    #         return constructor(*args, **kwargs)

    #     raise NotImplementedError(f"cannot instantiate {constructor}")

    def clone(self):
        clone = Pipeline()
        clone._definitions = dict(self._definitions)
        return clone

    def forget(self, key):
        if key in self._definitions:
            del self._definitions[key]
        if key in self._values:
            del self._values[key]


## class SurveyPipeline:
##     def __init__(self):
##         self._surfaces = []
##         self._template = PipelineTemplate()
## 
##     def configure(self, name, binding):
##         if name == "surfaces":
##             self._surfaces.extend(binding)
## 
##         self._template.bind(name, binding)
## 
##     def __iter__(self):
##         from more_itertools import roundrobin
## 
##         for surface in roundrobin(*self._surfaces):
##             yield self._template.create(surface)
## 
## 
## class PipelineTemplate:
##     def __init__(self):
##         self._factories = {}
## 
##     def register_named_factories(self, name, factories):
##         raise Exception
## 
##     def register_type_factory(self, type, factory):
##         raise Exception
## 
##     # def register_factory(self, name, factory):
##     #     if isinstance(factory, list):
##     #         if name not in self._factories:
##     #             self._factories
##     #         if name not in self._bindings:
##     #             self._bindings[name] = []
##     #         self._bindings[name].extend(binding)
##     #     else:
##     #         if name in self._bindings:
##     #             raise NotImplementedError(f"cannot handle multiple values for {name} yet")
##     #         self._bindings[name] = binding
## 
##     def create_pipeline(self, constants):
##         raise Exception
##         # return Pipeline({**self._bindings, "surface": surface})
## 
## 
## class Pipeline:
##     def __init__(self, template, constants):
##         self._template = template
##         self._values = dict(constants)
## 
##     def get(self, type, default=None):
##         raise Exception
## 
##     def get(self, name, default=None):
##         raise Exception
## 
##     def get(self, name, default=None):
##         raise Exception
##         if name not in self._values:
##             if name not in self._bindings:
##                 if default:
##                     return default(self)
## 
##                 raise ValueError(f"pipeline does not define {name!r}")
## 
##         self._bindings[name](self)
