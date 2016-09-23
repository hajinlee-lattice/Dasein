from .elb import ElasticLoadBalancer
from .iam import InstanceProfile
from .parameter import Parameter, ArnParameter
from .resource import Resource


class AutoScalingGroup(Resource):
    def __init__(self, logicalId, capacity, min_size, max_size):
        Resource.__init__(self, logicalId)
        self._template = {
            "Type" : "AWS::AutoScaling::AutoScalingGroup",
            "Properties" : {
                "VPCZoneIdentifier" : [ { "Ref" : "SubnetId1" }, {"Ref": "SubnetId2"}, {"Ref": "SubnetId3"} ],
                "Cooldown": "1200",
                "MinSize" : min_size.ref() if isinstance(min_size, Parameter) else min_size,
                "MaxSize" : max_size.ref() if isinstance(max_size, Parameter) else max_size,
                "DesiredCapacity" : capacity.ref() if isinstance(capacity, Parameter) else capacity
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
        assert isinstance(elb, ElasticLoadBalancer) or isinstance(elb, Parameter)
        if "LoadBalancerNames" not in self._template["Properties"]:
            self._template["Properties"]["LoadBalancerNames"] = []
        if isinstance(elb, ElasticLoadBalancer):
            self._template["Properties"]["LoadBalancerNames"].append(elb.get_name())
        else:
            self._template["Properties"]["LoadBalancerNames"].append(elb.ref())
        return self

    def attach_elbs(self, elbs):
        for elb in elbs:
            self.attache_elb(elb)
        return self

    def attach_tgrp(self, targetgroup):
        assert isinstance(targetgroup, ArnParameter)
        if "TargetGroupARNs" not in self._template["Properties"]:
            self._template["Properties"]["TargetGroupARNs"] = []
        self._template["Properties"]["TargetGroupARNs"].append(targetgroup.ref())
        return self

    def attach_tgrps(self, tgrps):
        for tgrp in tgrps:
            self.attach_tgrp(tgrp)
        return self


class LaunchConfiguration(Resource):
    def __init__(self, logicalId, instance_type_ref="InstanceType"):
        Resource.__init__(self, logicalId)
        self._template = {
            "Type": "AWS::AutoScaling::LaunchConfiguration",
            "Properties": {
                "ImageId" : {
                    "Fn::FindInMap": [ "AWSRegion2AMI", {"Ref": "AWS::Region"}, "AmazonECSLinux"]
                },
                "InstanceType"   : { "Ref" : instance_type_ref },
                "EbsOptimized" : { "Fn::FindInMap" : [ "Instance2Options", { "Ref" : "InstanceType" }, "EBSOptimized" ] },
                "InstanceMonitoring" : "true",
                "KeyName"        : { "Ref" : "KeyName" },
                "SecurityGroups" : [ { "Ref" : "SecurityGroupId" } ],
                "UserData"       : ""
            }
        }

    def set_instance_profile(self, profile):
        assert isinstance(profile, InstanceProfile) or isinstance(profile, ArnParameter)
        self._template["Properties"]["IamInstanceProfile"] = profile.ref()
        return self

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


class ScalingPolicy(Resource):
    def __init__(self, logicalId, asgroup):
        assert isinstance(asgroup, AutoScalingGroup)
        Resource.__init__(self, logicalId)
        self._template = {
            "Type" : "AWS::AutoScaling::ScalingPolicy",
            "Properties" : {
                "AutoScalingGroupName" : asgroup.ref()
            }
        }

class SimpleScalingPolicy(ScalingPolicy):
    def __init__(self, logicalId, asgroup, incremental):
        assert isinstance(incremental, int)
        ScalingPolicy.__init__(self, logicalId, asgroup)
        self._template["Properties"]["AdjustmentType"] = "ChangeInCapacity"
        self._template["Properties"]["ScalingAdjustment"] = str(incremental)

class PercentScalingPolicy(ScalingPolicy):
    def __init__(self, logicalId, asgroup, incremental):
        assert isinstance(incremental, int)
        ScalingPolicy.__init__(self, logicalId, asgroup)
        self._template["Properties"]["AdjustmentType"] = "PercentChangeInCapacity"
        self._template["Properties"]["ScalingAdjustment"] = str(incremental)
