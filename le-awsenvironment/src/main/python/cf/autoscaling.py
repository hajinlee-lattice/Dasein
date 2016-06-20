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
                            "01_add_instance_to_cluster" : {
                                "command" : { "Fn::Join": [ "", [ "#!/bin/bash\n", "echo ECS_CLUSTER=", ecscluster.ref(), " >> /etc/ecs/ecs.config" ] ] }
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
                                    "action=/opt/aws/bin/cfn-init -v ",
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
                "KeyName"        : { "Ref" : "KeyName" },
                "SecurityGroups" : [ { "Ref" : "SecurityGroupId" } ],
                "UserData"       : { "Fn::Base64" : { "Fn::Join" : ["", [
                    "#!/bin/bash -xe\n",
                    "yum install -y aws-cfn-bootstrap\n",

                    "/opt/aws/bin/cfn-init -v ",
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