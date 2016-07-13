import json
import os

from .iam import InstanceProfile
from .parameter import Parameter
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

        if instance_type is not None:
            self._template["Properties"]["InstanceType"] = instance_type

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


class SchemaRegistryEc2Instance(EC2Instance):
    def __init__(self, name, ecscluster, subnet_ref=None, instance_type=None):
        EC2Instance.__init__(name, subnet_ref, instance_type, os="AmazonECSLinux")
        self.set_metadata(self.__metadata(ecscluster))

    def __metadata(self, ecscluster):
        return {
            "AWS::CloudFormation::Init" : {
                "install" : {
                    "files" : {
                        "/etc/cfn/cfn-hup.conf" : {
                            "content" : { "Fn::Join" : ["", [
                                "[main]\n",
                                "stack=", { "Ref" : "AWS::StackId" }, "\n",
                                "region=", { "Ref" : "AWS::Region" }, "\n"
                            ]]},
                            "mode"    : "000400",
                            "owner"   : "root",
                            "group"   : "root"
                        },
                        "/etc/cfn/hooks.d/cfn-auto-reloader.conf" : {
                            "content": { "Fn::Join" : ["", [
                                "[cfn-auto-reloader-hook]\n",
                                "triggers=post.update\n",
                                "path=Resources.ContainerInstances.Metadata.AWS::CloudFormation::Init\n",
                                "action=/opt/aws/bin/cfn-init -v",
                                "         -c reload",
                                "         --stack ", { "Ref" : "AWS::StackName" },
                                "         --resource %s " % self.logical_id(),
                                "         --region ", { "Ref" : "AWS::Region" }, "\n",
                                "runas=root\n"
                            ]]}
                        }
                    },
                    "commands" : {
                        "01_save_ip" : {
                            "command" : { "Fn::Join": [ "\n", [
                                "#!/bin/bash",
                                "ADDR=`ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'`",
                                "echo $ADDR >> /etc/internaladdr.txt",
                                "PUBADDR=`curl http://169.254.169.254/latest/meta-data/public-ipv4`",
                                "echo $PUBADDR >> /etc/externaladdr.txt"
                            ] ] }
                        },
                        "02_add_instance_to_cluster" : {
                            "command" : { "Fn::Join": [ "", [
                                "#!/bin/bash\n",
                                "rm -rf /etc/ecs/ecs.config\n",
                                "touch /etc/ecs/ecs.config\n",
                                "echo ECS_CLUSTER=", ecscluster.ref(), " >> /etc/ecs/ecs.config\n",
                                "echo ECS_AVAILABLE_LOGGING_DRIVERS=[\\\"json-file\\\", \\\"awslogs\\\"] >> /etc/ecs/ecs.config\n",
                            ] ] }
                        }
                    },
                    "services" : {
                        "sysvinit" : {
                            "cfn-hup" : { "enabled" : "true", "ensureRunning" : "true", "files" : ["/etc/cfn/cfn-hup.conf", "/etc/cfn/hooks.d/cfn-auto-reloader.conf"] }
                        }
                    }
                }
            }
        }