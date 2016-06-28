import json
import os

TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'template')

class Template:
    def __init__(self):
        self._template =  {}

    def _merge_into_attr(self, attr, data):
        self._initialize_attr(attr)
        for k, v in data.items():
            self._template[attr][k] = v

    def _initialize_attr(self, attr):
        if attr not in self._template:
            self._template[attr] = {}

    def template(self):
        return self._template

    def json(self):
        return json.dumps(self._template, indent=2, separators=(',', ': '))