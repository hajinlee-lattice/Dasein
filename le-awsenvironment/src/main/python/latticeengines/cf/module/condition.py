from .template import Template

class Condition(Template):
    def __init__(self, name, func):
        Template.__init__(self)
        self._name = name
        self._template = { name : {} }
        self.set_func(func)

    def set_func(self, func):
        self._template[self._name] = func
        return self

    def get_name(self):
        return self._name

    def ref(self):
        return { "Ref" : self._name }