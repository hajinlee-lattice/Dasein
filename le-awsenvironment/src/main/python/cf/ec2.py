import json
import os

from .resource import Resource
from .template import TEMPLATE_DIR

SECURITY_GROUP="EC2SecurityGroup"

def _ec2_mappings():
    json_file = os.path.join(TEMPLATE_DIR, 'common', 'ec2_mappings.json')
    with open(json_file) as f:
        return json.load(f)


def _ec2_params():
    json_file = os.path.join(TEMPLATE_DIR, 'common', 'ec2_params.json')
    with open(json_file) as f:
        return json.load(f)


def _ec2_security_group():
    return {
        "Type" : "AWS::EC2::SecurityGroup",
        "Properties" : {
            "GroupDescription" : "Enable all access to trust ips",
            "SecurityGroupIngress" : [
                {"IpProtocol" : "tcp", "FromPort" : "0", "ToPort" : "65535", "CidrIp" : { "Ref" : "TrustedIPZone"}}
            ],
            "VpcId": { "Ref": "VpcId" }
        }
    }


class EC2Instance(Resource):
    def __init__(self, name, server_type="default", subnet_ref=None):
        Resource.__init__(self)
        self.__name = name
        self._template = {
            "Type": "AWS::EC2::Instance",
            "Properties": {
                "ImageId": EC2Instance.__image_id(server_type),
                "InstanceType": {"Ref": "InstanceType"},
                "SecurityGroupIds": [{ "Ref": "SecurityGroupId" }],
                "SubnetId": { "Ref": subnet_ref },
                "KeyName": { "Ref" : "KeyName" },
                "UserData": EC2Instance.__userdata(name),
                "Tags": []
            },
            "CreationPolicy": {
                "ResourceSignal": {
                    "Timeout": "PT5M"
                }
            }
        }

    def metadata(self, metadata):
        self._template["Metadata"] = metadata
        return self

    def get_name(self):
        return self.__name

    @classmethod
    def __userdata(cls, name):
        json_file = os.path.join(TEMPLATE_DIR, 'common', 'ec2_userdata.json')
        with open(json_file) as f:
            text = f.read().replace('${INSTANCE_NAME}', name)
            return json.loads(text)

    @classmethod
    def __image_id(cls, server_type="default"):
        return {
            "Fn::FindInMap": [ "AWSRegion2AMI", { "Ref": "AWS::Region" }, {
                "Fn:FindInMap": [ "ServerType2AMIType", server_type, "AMIType" ]
            } ]
        }
