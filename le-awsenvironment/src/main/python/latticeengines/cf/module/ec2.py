import json
import os

from .iam import InstanceProfile
from .parameter import Parameter
from .resource import Resource
from .template import TEMPLATE_DIR

TYPE_DEF = {
    "t2.micro":   { "core": 1, "mem_gb": 1,     "on_demand": 0.013 },
    "t2.medium":  { "core": 2, "mem_gb": 4,     "on_demand": 0.052,  "ecs_mem" : 3956 },
    "m4.large":   { "core": 2, "mem_gb": 8,     "on_demand": 0.120 },
    "m4.xlarge":  { "core": 4, "mem_gb": 16,    "on_demand": 0.239 },
    "m4.2xlarge": { "core": 8, "mem_gb": 32,    "on_demand": 0.479 },
    "m3.medium":  { "core": 1, "mem_gb": 3.75,  "on_demand": 0.067 },
    "m3.large":   { "core": 2, "mem_gb": 7.5,   "on_demand": 0.133 },
    "m3.xlarge":  { "core": 4, "mem_gb": 15,    "on_demand": 0.266 },
    "m3.2xlarge": { "core": 8, "mem_gb": 30,    "on_demand": 0.532 },
    "r3.large":   { "core": 2, "mem_gb": 15.25, "on_demand": 0.166 },
    "r3.xlarge":  { "core": 4, "mem_gb": 30.5,  "on_demand": 0.333 },
    "r3.2xlarge": { "core": 8, "mem_gb": 61,    "on_demand": 0.665 }
}

def _ec2_mappings():
    json_file = os.path.join(TEMPLATE_DIR, 'common', 'ec2_mappings.json')
    with open(json_file) as f:
        return json.load(f)


def _ec2_params():
    json_file = os.path.join(TEMPLATE_DIR, 'common', 'ec2_params.json')
    with open(json_file) as f:
        return json.load(f)


class EC2Instance(Resource):
    def __init__(self, name, subnet_ref, instance_type, ec2_key, os="AmazonLinux"):
        assert isinstance(instance_type, Parameter)
        assert isinstance(ec2_key, Parameter)
        Resource.__init__(self, name)
        self._template = {
            "Type": "AWS::EC2::Instance",
            "Properties": {
                "ImageId": EC2Instance.__image_id(os),
                "InstanceType": instance_type.ref(),
                "SecurityGroupIds": [ ],
                "SubnetId": { "Ref": subnet_ref },
                "Monitoring": "true",
                "KeyName": ec2_key.ref(),
                "UserData": EC2Instance.__userdata(name),
                "Tags": []
            },
            "CreationPolicy": {
                "ResourceSignal": {
                    "Timeout": "PT20M"
                }
            }
        }

    def set_instanceprofile(self, instanceprofile):
        assert isinstance(instanceprofile, InstanceProfile)
        self._template["Properties"]["IamInstanceProfile"] = instanceprofile.ref()
        return self

    def set_metadata(self, metadata):
        self._template["Metadata"] = metadata
        return self

    def add_sg(self, sg):
        assert isinstance(sg, Parameter)
        self._template["Properties"]["SecurityGroupIds"].append(sg.ref())
        return self

    def userdata(self, userdata):
        self._template["Properties"]["UserData"] = userdata
        return self

    def mount(self, device, size, type='gp2'):
        if "BlockDeviceMappings" not in self._template["Properties"]:
            self._template["Properties"]["BlockDeviceMappings"] = []
        device = {
            "DeviceName" : device,
            "Ebs" : {
                "DeleteOnTermination" : "true",
                "VolumeSize" : size,
                "VolumeType" : type
            }
        }
        self._template["Properties"]["BlockDeviceMappings"].append(device)
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

class Volume(Resource):
    def __init__(self, logicalId, size, type):
        Resource.__init__(self, logicalId)
        self._template = {
            "Type":"AWS::EC2::Volume",
            "Properties" : {
                "Size" : size,
                "VolumeType" : type
            }
        }

    def for_ec2(self, ec2):
        assert isinstance(ec2, EC2Instance)
        self._template["Properties"]["AvailabilityZone"] = { "Fn::GetAtt" : [ ec2.logical_id(), "AvailabilityZone" ] }
        return self

class VolumeAttachement(Resource):
    def __init__(self, logicalId, instance, volume, device):
        assert isinstance(instance, EC2Instance)
        assert isinstance(volume, Volume)
        Resource.__init__(self, logicalId)
        self._template = {
            "Type":"AWS::EC2::VolumeAttachment",
            "Properties" : {
                "Device" : device,
                "InstanceId" : instance.ref(),
                "VolumeId" : volume.ref()
            }
        }