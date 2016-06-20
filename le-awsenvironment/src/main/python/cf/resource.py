from .template import Template

class Resource(Template):

    def __init__(self, logicalId):
        Template.__init__(self)
        self._logicalId = logicalId
        self._template =  {
            "Properties": {
                "Tags": []
            }
        }

    def logical_id(self):
        return self._logicalId

    def add_dependency(self, resource):
        assert isinstance(resource, Resource)
        if "DependsOn" not in self._template:
            self._template["DependsOn"] = [ resource.logical_id() ]
        else:
            self._template["DependsOn"].append(resource.logical_id())

    def add_tag(self, key, value):
        self._template["Properties"]["Tags"].append({
            "Key": key,
            "Value": value
        })
        return self

    def ref(self):
        return { "Ref" : self._logicalId }