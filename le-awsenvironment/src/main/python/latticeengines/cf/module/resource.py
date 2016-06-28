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

    def depends_on(self, resource):
        assert isinstance(resource, Resource)
        if "DependsOn" not in self._template:
            self._template["DependsOn"] = [ resource.logical_id() ]
        else:
            self._template["DependsOn"].append(resource.logical_id())
        return self

    def add_tag(self, key, value):
        self._template["Properties"]["Tags"].append({
            "Key": key,
            "Value": value
        })
        return self

    def require(self, condition):
        assert isinstance(condition, Condition)
        self._template["Condition"] = condition.get_name()
        return self

    def ref(self):
        return { "Ref" : self._logicalId }