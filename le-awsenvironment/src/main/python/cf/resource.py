from .template import Template

class Resource(Template):
    def __init__(self):
        Template.__init__(self)
        self._template =  {
            "Properties": {
                "Tags": []
            }
        }

    def add_tag(self, key, value):
        self._template["Properties"]["Tags"].append({
            "Key": key,
            "Value": value
        })
        return self