from .template import Template


class Volume(Template):
    def __init__(self, name, path):
        Template.__init__(self)
        self._template =  {
            'name': name,
            'host': {
                'sourcePath': path
            }
        }

