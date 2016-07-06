from .parameter import Parameter
from .resource import Resource


class EfsFileSystem(Resource):
    def __init__(self, logicalId, name):
        Resource.__init__(self, logicalId)
        self._template = {
            "Type" : "AWS::EFS::FileSystem",
            "Properties" : {
                "FileSystemTags" : [ {
                    "Key" : "Name",
                    "Value" : name
                } ]
            }
        }

class EfsMountTarget(Resource):
    def __init__(self, logicalId, efs, efs_sg, subnet_id):
        assert isinstance(efs, EfsFileSystem)
        assert isinstance(efs_sg, Parameter)
        assert isinstance(subnet_id, Parameter)

        Resource.__init__(self, logicalId)
        self._template = {
            "Type" : "AWS::EFS::MountTarget",
            "Properties" : {
                "FileSystemId" : efs.ref(),
                "SecurityGroups" : [ efs_sg.ref() ],
                "SubnetId" : subnet_id.ref()
            }
        }