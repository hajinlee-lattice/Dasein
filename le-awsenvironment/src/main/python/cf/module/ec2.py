import json
import os

from .iam import InstanceProfile
from .resource import Resource
from .template import TEMPLATE_DIR


def _ec2_mappings():
    json_file = os.path.join(TEMPLATE_DIR, 'common', 'ec2_mappings.json')
    with open(json_file) as f:
        return json.load(f)


def _ec2_params():
    json_file = os.path.join(TEMPLATE_DIR, 'common', 'ec2_params.json')
    with open(json_file) as f:
        return json.load(f)


class EC2Instance(Resource):
    def __init__(self, name, subnet_ref=None, instance_type=None, os="AmazonLinux"):
        Resource.__init__(self, name)
        self._template = {
            "Type": "AWS::EC2::Instance",
            "Properties": {
                "ImageId": EC2Instance.__image_id(os),
                "InstanceType": { "Ref": "InstanceType" },
                "SecurityGroupIds": [{ "Ref": "SecurityGroupId" }],
                "SubnetId": { "Ref": subnet_ref },
                "Monitoring": "true",
                "KeyName": { "Ref" : "KeyName" },
                "UserData": EC2Instance.__userdata(name),
                "Tags": []
            },
            "CreationPolicy": {
                "ResourceSignal": {
                    "Timeout": "PT20M"
                }
            }
        }

        if instance_type is not None:
            self._template["Properties"]["InstanceType"] = instance_type

    def set_instanceprofile(self, instanceprofile):
        assert isinstance(instanceprofile, InstanceProfile)
        self._template["Properties"]["IamInstanceProfile"] = instanceprofile.ref()
        return self

    def metadata(self, metadata):
        self._template["Metadata"] = metadata
        return self

    def userdata(self, userdata):
        self._template["Properties"]["UserData"] = userdata
        return self

    @classmethod
    def __userdata(cls, name):
        json_file = os.path.join(TEMPLATE_DIR, 'common', 'ec2_userdata.json')
        with open(json_file) as f:
            text = f.read().replace('${INSTANCE_NAME}', name)
            return json.loads(text)

    @classmethod
    def __image_id(cls, os):
        return {
            "Fn::FindInMap": [ "AWSRegion2AMI", {"Ref": "AWS::Region"}, os ]
        }
