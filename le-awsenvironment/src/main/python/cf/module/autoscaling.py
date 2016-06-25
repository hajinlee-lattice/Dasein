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

    def set_capacity(self, capacity):
        self._template["Properties"]["DesiredCapacity"] = capacity
        return self

    def set_max_size(self, max_size):
        self._template["Properties"]["MaxSize"] = max_size
        return self

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
    def __init__(self, logicalId, instanceprofile, instance_type_ref="InstanceType"):
        assert isinstance(instanceprofile, InstanceProfile)

        Resource.__init__(self, logicalId)
        self._template = {
            "Type": "AWS::AutoScaling::LaunchConfiguration",
            "Properties": {
                "ImageId" : {
                    "Fn::FindInMap": [ "AWSRegion2AMI", {"Ref": "AWS::Region"}, "AmazonECSLinux"]
                },
                "InstanceType"   : { "Ref" : instance_type_ref },
                "IamInstanceProfile": instanceprofile.ref(),
                "EbsOptimized" : { "Fn::FindInMap" : [ "Instance2Options", { "Ref" : "InstanceType" }, "EBSOptimized" ] },
                "InstanceMonitoring" : "true",
                "KeyName"        : { "Ref" : "KeyName" },
                "SecurityGroups" : [ { "Ref" : "SecurityGroupId" } ],
                "UserData"       : ""
            }
        }

    def set_metadata(self, metadata):
        self._template["Metadata"] = metadata
        return self

    def set_userdata(self, userdata):
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





