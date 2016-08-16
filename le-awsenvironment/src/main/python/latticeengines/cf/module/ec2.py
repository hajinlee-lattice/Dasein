import json
import os

from .iam import InstanceProfile
from .parameter import Parameter
from .resource import Resource
from .template import TEMPLATE_DIR


def ec2_defn():
    json_file = os.path.join(TEMPLATE_DIR, 'common', 'ec2_definition.json')
    with open(json_file) as f:
        return json.load(f)

class EC2Instance(Resource):
    def __init__(self, name, instance_type, ec2_key, os="AmazonLinux"):
        assert isinstance(instance_type, Parameter)
        assert isinstance(ec2_key, Parameter)
        Resource.__init__(self, name)
        self._template = {
            "Type": "AWS::EC2::Instance",
            "Properties": {
                "ImageId": EC2Instance.__image_id(os),
                "InstanceType": instance_type.ref(),
                "SecurityGroupIds": [ ],
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

    def set_subnet(self, subnet):
        assert isinstance(subnet, Parameter)
        self._template["Properties"]["SubnetId"] = subnet.ref()
        return self

    def set_private_ip(self, private_ip):
        self._template["Properties"]["PrivateIpAddress"] = private_ip
        return self

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