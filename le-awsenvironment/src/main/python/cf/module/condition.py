from .template import Template

class Condition(Template):
    def __init__(self, name):
        Template.__init__(self)
        self._name = name
        self._template = { name : {} }

    def set_func(self, func):
        self._template[self._name] = func
        return self

    def ref(self):
        return { "Ref" : self._name }