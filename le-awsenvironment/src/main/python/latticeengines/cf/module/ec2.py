import json
import os

from .iam import InstanceProfile
from .parameter import Parameter
from .resource import Resource
from .template import TEMPLATE_DIR
from ...conf import AwsEnvironment


def ec2_defn():
    json_file = os.path.join(TEMPLATE_DIR, 'common', 'ec2_definition.json')
    with open(json_file) as f:
        return json.load(f)

def ecs_metadata(ec2, ecscluster, efs, env, instance_role_name):
    config = AwsEnvironment(env)
    lerepo = config.le_repo()
    bucket = config.cf_bucket()
    chefbucket= config.chef_bucket()
    ssh_group = config.iam_ssh_group()
    cert = "star.lattice.local"

    assert isinstance(instance_role_name, Parameter)

    md = {
        "AWS::CloudFormation::Authentication":{
            "S3AccessCreds":{
                "type":"S3",
                "roleName": instance_role_name.ref()
            }
        },
        "AWS::CloudFormation::Init" : {
            "configSets": {
                "bootstrap": [ "install" ],
                "reload": [ "install" ]
            },
            "install" : {
                "packages" : {
                    "yum" : {
                        "xfsprogs" : [],
                        "nfs-utils": [],
                        "aws-cli": [],
                        "jq": []
                    }
                },
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
                            "         --resource %s " % ec2.logical_id(),
                            "         --region ", { "Ref" : "AWS::Region" }, "\n",
                            "runas=root\n"
                        ]]}
                    },
                    "/etc/ledp/s-iss.pub":{
                        "source":"http://" + config.s3_endpoint() + "/" + chefbucket + "/ssh_keys/s-iss/pub",
                        "mode":"000777",
                        "owner":"root",
                        "group":"root",
                        "authentication":"S3AccessCreds"
                    },
                    "/etc/ledp/lattice.crt":{
                        "source":"http://" + config.s3_endpoint() + "/" + chefbucket + "/tls/" + cert + "/" + cert + ".crt",
                        "mode":"000600",
                        "owner":"root",
                        "group":"root",
                        "authentication":"S3AccessCreds"
                    },
                    "/etc/ledp/lattice.key":{
                        "source":"http://" + config.s3_endpoint() + "/" + chefbucket + "/tls/" + cert + "/" + cert + ".key",
                        "mode":"000600",
                        "owner":"root",
                        "group":"root",
                        "authentication":"S3AccessCreds"
                    },
                    "/etc/ledp/ledp_keystore.jks":{
                        "source":"http://" + config.s3_endpoint() + "/" + chefbucket + "/tls/ledp_keystore.jks",
                        "mode":"000600",
                        "owner":"root",
                        "group":"root",
                        "authentication":"S3AccessCreds"
                    },
                    "/etc/ledp/cacerts":{
                        "source":"http://" + config.s3_endpoint() + "/" + chefbucket + "/tls/cacerts",
                        "mode":"000600",
                        "owner":"root",
                        "group":"root",
                        "authentication":"S3AccessCreds"
                    },
                    "/etc/ledp/createSSHAccounts.sh":{
                        "source":"http://" + config.s3_endpoint() + "/" + bucket + "/ssh/createAccounts.sh",
                        "mode":"000777",
                        "owner":"root",
                        "group":"root",
                        "authentication":"S3AccessCreds"
                    },
                    "/etc/yum.repos.d/le.repo": {
                        "content": {
                            "Fn::Join": [
                                "",
                                [ '[lattice]\n',
                                  'name=Lattice Engines Development Repo\n',
                                  'baseurl=', lerepo,'/6.7/os/x86_64\n',
                                  '#mirrorlist=https://mirrors.fedoraproject.org/metalink?repo=epel-6&arch=$basearch\n',
                                  '#failovermethod=priority\n',
                                  'priority=1\n',
                                  'enabled=1\n',
                                  'gpgcheck=1\n',
                                  'gpgkey=', lerepo,'/RPM-GPG-KEY\n',
                                  'assumeyes=1\n'
                                  ]
                            ]
                        },
                        "mode": "000755",
                        "owner": "root",
                        "group": "root"
                    },
                    "/etc/telegraf/telegraf.conf":{
                        "source":"http://" + config.s3_endpoint() + "/" + bucket + "/telegraf/telegraf.conf",
                        "mode":"000777",
                        "owner":"root",
                        "group":"root",
                        "authentication":"S3AccessCreds"
                    },
                    "/tmp/mount_efs.sh": {
                        "content": {
                            "Fn::Join": [
                                "",
                                [ "#!/usr/bin/env bash \n",
                                  "mkdir -p /mnt/efs \n",
                                  "echo \"", config.lpi_efs_ip_1(), "\" > /tmp/", config.subnet_az_1(), ".ip\n",
                                  "echo \"", config.lpi_efs_ip_2(), "\" > /tmp/", config.subnet_az_2(), ".ip\n",
                                  "echo \"", config.lpi_efs_ip_3(), "\" > /tmp/", config.subnet_az_3(), ".ip\n",
                                  "for i in {1..100}; do\n",
                                  "    az=$(curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone)\n",
                                  "    if [ ! -z \"${az}\" ]; then\n",
                                  "        break;\n",
                                  "    fi;\n",
                                  "    echo \"did not find availability zone, retry after 1 second\"\n",
                                  "    sleep 1;\n",
                                  "done;\n",
                                  "efs_ip=`cat /tmp/${az}.ip`\n",
                                  "echo ${efs_ip} > /etc/efsip.txt\n",
                                  "cat /etc/efsip.txt\n",
                                  "echo \"${efs_ip}:/ /mnt/efs nfs4 nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 0 0\" >> /etc/fstab \n"
                                  ]
                            ]
                        },
                        "mode": "000777",
                        "owner": "root",
                        "group": "root"
                    }
                },
                "commands" : {
                    "00_yum_update" : {
                        "command" : { "Fn::Join": [ "\n", [
                            "#!/bin/bash",
                            "yum update -y"
                        ] ] }
                    },
                    "01_save_ip" : {
                        "command" : { "Fn::Join": [ "\n", [
                            "#!/bin/bash",
                            "ADDR=`ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'`",
                            "echo $ADDR >> /etc/internaladdr.txt",
                            "PUBADDR=`curl http://169.254.169.254/latest/meta-data/public-ipv4`",
                            "echo $PUBADDR >> /etc/externaladdr.txt"
                        ] ] }
                    },
                    "03_le_dirs" : {
                        "command" : { "Fn::Join": [ "\n", [
                            "#!/bin/bash",
                            "mkdir -p /etc/ledp",
                            "chmod 777 /etc/ledp",
                            "mkdir -p /var/log/ledp",
                            "chmod 777 /var/log/ledp",
                            "mkdir -p /var/cache/scoringapi",
                            "chmod 777 /var/cache/scoringapi"
                        ] ] }
                    },
                    "04_le_yum_repo" : {
                        "command" : { "Fn::Join": [ "\n", [
                            "#!/bin/bash",
                            "yum clean all",
                            "yum makecache",
                            "yum install -y lce_client",
                            "chkconfig lce_client on",
                            "/opt/lce_client/set-server-ip.sh 10.51.1.40 31300",
                        ] ] }
                    },
                    "05_iss_user" : {
                        "command" : { "Fn::Join": [ "\n", [
                            "#!/bin/bash",
                            "useradd s-iss",
                            "mkdir -p /home/s-iss/.ssh",
                            "chmod 0700 /home/s-iss/.ssh",
                            "mv /etc/ledp/s-iss.pub .",
                            "cat s-iss.pub > /home/s-iss/.ssh/authorized_keys",
                            "chmod 0600 /home/s-iss/.ssh/authorized_keys",
                            "rm -f s-iss.pub"
                        ] ] }
                    },
                    "06_ssh_user" : {
                        "command" : { "Fn::Join": [ "\n", [
                            "#!/bin/bash",
                            "bash /etc/ledp/createSSHAccounts.sh %s || true" % ssh_group
                        ] ] }
                    },
                    "07_concat_tls" : {
                        "command" : { "Fn::Join": [ "\n", [
                            "#!/bin/bash",
                            "cd /etc/ledp",
                            "cat lattice.crt lattice.key > lattice.pem",
                            "chmod 600 lattice.pem"
                        ] ] }
                    },
                    "10_add_instance_to_cluster" : {
                        "command" : { "Fn::Join": [ "", [
                            "#!/bin/bash\n",
                            "rm -rf /etc/ecs/ecs.config\n",
                            "touch /etc/ecs/ecs.config\n",
                            "echo ECS_CLUSTER=", ecscluster.ref(), " >> /etc/ecs/ecs.config\n",
                            "echo ECS_AVAILABLE_LOGGING_DRIVERS=[\\\"json-file\\\", \\\"awslogs\\\",\\\"splunk\\\"] >> /etc/ecs/ecs.config\n",
                            "echo ECS_RESERVED_PORTS=[22] >> /etc/ecs/ecs.config\n"
                        ] ] }
                    },
                    "20_start_telegraf" : {
                        "command" : { "Fn::Join": [ "", [
                            "start ecs\n"
                            "for i in {1..100}; do\n",
                            "    instance_arn=`curl -s http://localhost:51678/v1/metadata | jq -r '. | .ContainerInstanceArn' | awk -F/ '{print $NF}'`\n",
                            "    if [ ! -z \"${instance_arn}\" ]; then\n",
                            "        break;\n",
                            "    fi;\n",
                            "    echo \"did not find instance arn, retry after 1 second\"\n",
                            "    sleep 1;\n",
                            "done;\n",
                            "region=", { "Ref" : "AWS::Region" }, "\n",
                            "aws ecs start-task --cluster ", ecscluster.ref(), " --task-definition telegraf --container-instances ${instance_arn} --region ${region}\n"
                        ] ] }
                    },
                    "30_mount_efs" : {
                        "command" : { "Fn::Join": [ "\n", [
                            "bash /tmp/mount_efs.sh",
                            "mount -a"
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

    if efs is None:
        md["AWS::CloudFormation::Init"]["install"]["packages"] = {
            "yum" : {
                "aws-cli": [],
                "jq": []
            }
        }
        del md["AWS::CloudFormation::Init"]["install"]["files"]["/tmp/mount_efs.sh"]
        del md["AWS::CloudFormation::Init"]["install"]["commands"]["30_mount_efs"]

    return md


class EC2Instance(Resource):
    def __init__(self, name, instance_type, ec2_key, os="AmazonLinux"):
        assert isinstance(instance_type, Parameter)
        assert isinstance(ec2_key, Parameter)
        Resource.__init__(self, name)
        self._template = {
            "Type": "AWS::EC2::Instance",
            "Properties": {
                "ImageId": EC2Instance._image_id(os),
                "InstanceType": instance_type.ref(),
                "SecurityGroupIds": [ ],
                "Monitoring": "false",
                "KeyName": ec2_key.ref(),
                "UserData": EC2Instance.__userdata(name),
                "Tags": []
            },
            "CreationPolicy": {
                "ResourceSignal": {
                    "Timeout": "PT10M"
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
        if isinstance(instanceprofile, InstanceProfile) or isinstance(instanceprofile, Parameter):
            self._template["Properties"]["IamInstanceProfile"] = instanceprofile.ref()
        else:
            self._template["Properties"]["IamInstanceProfile"] = instanceprofile
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
    def _image_id(cls, os):
        return {
            "Fn::FindInMap": [ "AWSRegion2AMI", {"Ref": "AWS::Region"}, os ]
        }


class ECSInstance(EC2Instance):
    def __init__(self, name, instance_type, ec2_key, instance_profile_name, ecscluster, efs, env, instance_role_name):
        assert isinstance(instance_type, Parameter)
        assert isinstance(ec2_key, Parameter)
        Resource.__init__(self, name)
        self._template = {
            "Type": "AWS::EC2::Instance",
            "Properties": {
                "ImageId": EC2Instance._image_id("AmazonECSLinux"),
                "InstanceType": instance_type.ref(),
                "SecurityGroupIds": [ ],
                "Monitoring": "true",
                "KeyName": ec2_key.ref(),
                "UserData": self.__ecs_userdata(),
                "Tags": []
            },
            "CreationPolicy": {
                "ResourceSignal": {
                    "Timeout": "PT10M"
                }
            }
        }
        self.set_metadata(ecs_metadata(self, ecscluster, efs, env, instance_role_name))
        self.set_instanceprofile(instance_profile_name)

    def __ecs_userdata(self):
        return { "Fn::Base64" : { "Fn::Join" : ["", [
            "#!/bin/bash -xe\n",
            "yum install -y aws-cfn-bootstrap\n",

            "/opt/aws/bin/cfn-init -v",
            "         -c bootstrap"
            "         --stack ", { "Ref" : "AWS::StackName" },
            "         --resource %s " % self.logical_id(),
            "         --region ", { "Ref" : "AWS::Region" }, "\n",

            "/opt/aws/bin/cfn-signal -e $? ",
            "         --stack ", { "Ref" : "AWS::StackName" },
            "         --resource %s " % self.logical_id(),
            "         --region ", { "Ref" : "AWS::Region" }, "\n"
        ]]}}

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