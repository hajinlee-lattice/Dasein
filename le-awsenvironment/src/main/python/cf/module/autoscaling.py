from .ecs import ECSCluster
from .elb import ElasticLoadBalancer
from .iam import InstanceProfile
from .resource import Resource


class AutoScalingGroup(Resource):
    def __init__(self, logicalId):
        Resource.__init__(self, logicalId)
        self._template = {
            "Type" : "AWS::AutoScaling::AutoScalingGroup",
            "Properties" : {
                "VPCZoneIdentifier" : [ { "Ref" : "SubnetId1" }, {"Ref": "SubnetId2"} ],
                "MinSize" : "1",
                "MaxSize" : { "Ref" : "MaxSize" },
                "DesiredCapacity" : { "Ref" : "DesiredCapacity" }
            },
            "CreationPolicy" : {
                "ResourceSignal" : {
                    "Timeout" : "PT15M"
                }
            },
            "UpdatePolicy": {
                "AutoScalingRollingUpdate": {
                    "MinInstancesInService": "1",
                    "MaxBatchSize": "1",
                    "PauseTime" : "PT15M",
                    "WaitOnResourceSignals": "true"
                }
            }
        }

    def set_min_size(self, min_size):
        self._template["Properties"]["MinSize"] = min_size
        self._template["UpdatePolicy"]["MinInstancesInService"] = min_size
        return self

    def add_pool(self, launchconfig):
        assert isinstance(launchconfig, LaunchConfiguration)
        self._merge_into_attr("Properties", { "LaunchConfigurationName" : launchconfig.ref() })
        return self

    def attache_elb(self, elb):
        assert isinstance(elb, ElasticLoadBalancer)
        if "LoadBalancerNames" not in self._template["Properties"]:
            self._template["Properties"]["LoadBalancerNames"] = []
        self._template["Properties"]["LoadBalancerNames"].append(elb.get_name())
        return self

    def attach_elbs(self, elbs):
        for elb in elbs:
            self.attache_elb(elb)
        return self

class LaunchConfiguration(Resource):
    def __init__(self, logicalId, ecscluster, asgroup, instanceprofile):
        assert isinstance(ecscluster, ECSCluster)
        assert isinstance(asgroup, AutoScalingGroup)
        assert isinstance(instanceprofile, InstanceProfile)

        Resource.__init__(self, logicalId)
        self.__asgroup = asgroup
        self._template = {
            "Type": "AWS::AutoScaling::LaunchConfiguration",
            "Metadata" : {
                "AWS::CloudFormation::Init" : {
                    "config" : {
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
                                    "         --stack ", { "Ref" : "AWS::StackName" },
                                    "         --resource %s " % self.logical_id(),
                                    "         --region ", { "Ref" : "AWS::Region" }, "\n",
                                    "runas=root\n"
                                ]]}
                            }
                        },

                        "services" : {
                            "sysvinit" : {
                                "cfn-hup" : { "enabled" : "true", "ensureRunning" : "true", "files" : ["/etc/cfn/cfn-hup.conf", "/etc/cfn/hooks.d/cfn-auto-reloader.conf"] }
                            }
                        }
                    }
                }
            },
            "Properties": {
                "ImageId" : {
                    "Fn::FindInMap": [ "AWSRegion2AMI", {"Ref": "AWS::Region"}, "AmazonECSLinux"]
                },
                "InstanceType"   : { "Ref" : "InstanceType" },
                "IamInstanceProfile": instanceprofile.ref(),
                "InstanceMonitoring" : "true",
                "KeyName"        : { "Ref" : "KeyName" },
                "SecurityGroups" : [ { "Ref" : "SecurityGroupId" } ],
                "UserData"       : { "Fn::Base64" : { "Fn::Join" : ["", [
                    "#!/bin/bash -xe\n",
                    "yum install -y aws-cfn-bootstrap\n",

                    "/opt/aws/bin/cfn-init -v",
                    "         --stack ", { "Ref" : "AWS::StackName" },
                    "         --resource %s " % self.logical_id(),
                    "         --region ", { "Ref" : "AWS::Region" }, "\n",

                    "/opt/aws/bin/cfn-signal -e $? ",
                    "         --stack ", { "Ref" : "AWS::StackName" },
                    "         --resource %s " % self.__asgroup.logical_id(),
                    "         --region ", { "Ref" : "AWS::Region" }, "\n"
                ]]}}
            }
        }

    def set_metadata(self, metadata):
        self._template["Metadata"] = metadata
        return self