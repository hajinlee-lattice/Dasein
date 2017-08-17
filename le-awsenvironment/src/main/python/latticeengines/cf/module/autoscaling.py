from .ecs import ECSCluster, ECSService
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
                "DesiredCapacity" : capacity.ref() if isinstance(capacity, Parameter) else capacity,
                "MetricsCollection" : [ { "Granularity" : "1Minute" } ],
            },
            "CreationPolicy" : {
                "ResourceSignal" : {
                    "Timeout" : "PT10M"
                }
            },
            "UpdatePolicy": {
                "AutoScalingRollingUpdate": {
                    "MinInstancesInService": "1",
                    "MaxBatchSize": "1",
                    "PauseTime" : "PT10M",
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

    def hook_notifications_to_sns_topic(self, sns_topic_arn):
        if "NotificationConfigurations" not in self._template["Properties"]:
            self._template["Properties"]["NotificationConfigurations"] = []
        notification = {
            "NotificationTypes" : [
                "autoscaling:EC2_INSTANCE_LAUNCH",
                "autoscaling:EC2_INSTANCE_LAUNCH_ERROR",
                "autoscaling:EC2_INSTANCE_TERMINATE",
                "autoscaling:EC2_INSTANCE_TERMINATE_ERROR",
                "autoscaling:TEST_NOTIFICATION"
            ],
            "TopicARN" : sns_topic_arn
        }
        if isinstance(sns_topic_arn, Parameter):
            notification["TopicARN"] = sns_topic_arn.ref()
        self._template["Properties"]["NotificationConfigurations"].append(notification)
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
                "InstanceMonitoring" : "false",
                "KeyName"        : { "Ref" : "KeyName" },
                "SecurityGroups" : [ { "Ref" : "SecurityGroupId" } ],
                "UserData"       : ""
            }
        }

    def set_instance_profile(self, profile):
        assert isinstance(profile, InstanceProfile) or isinstance(profile, Parameter)
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

    def cooldown(self, cooldown):
        self._template["Properties"]["Cooldown"] = cooldown
        return self

class SimpleScalingPolicy(ScalingPolicy):
    def __init__(self, logicalId, asgroup, incremental):
        ScalingPolicy.__init__(self, logicalId, asgroup)
        self._template["Properties"]["AdjustmentType"] = "ChangeInCapacity"
        if isinstance(incremental, Parameter):
            self._template["Properties"]["ScalingAdjustment"] = incremental.ref()
        else:
            self._template["Properties"]["ScalingAdjustment"] = str(incremental)

class ExactScalingPolicy(ScalingPolicy):
    def __init__(self, logicalId, asgroup, size):
        ScalingPolicy.__init__(self, logicalId, asgroup)
        self._template["Properties"]["AdjustmentType"] = "ExactCapacity"
        if isinstance(size, Parameter):
            self._template["Properties"]["ScalingAdjustment"] = size.ref()
        else:
            self._template["Properties"]["ScalingAdjustment"] = str(size)

class PercentScalingPolicy(ScalingPolicy):
    def __init__(self, logicalId, asgroup, percent):
        ScalingPolicy.__init__(self, logicalId, asgroup)
        self._template["Properties"]["AdjustmentType"] = "PercentChangeInCapacity"
        if isinstance(percent, Parameter):
            self._template["Properties"]["ScalingAdjustment"] = percent.ref()
        else:
            self._template["Properties"]["ScalingAdjustment"] = str(percent)


class ScalableTarget(Resource):
    def __init__(self, logicalId, resourceId, mincap, maxcap, autoscalearn):
        Resource.__init__(self, logicalId)
        self._template = {
            "Type" : "AWS::ApplicationAutoScaling::ScalableTarget",
            "Properties" : {
                "MaxCapacity" : maxcap.ref() if isinstance(maxcap, Parameter) else maxcap,
                "MinCapacity" : mincap.ref() if isinstance(mincap, Parameter) else mincap,
                "ResourceId" : resourceId.ref() if isinstance(resourceId, Parameter) else resourceId,
                "RoleARN" : autoscalearn.ref(),
                "ScalableDimension" : "ecs:service:DesiredCount",
                "ServiceNamespace" : "ecs"
            }
        }

class ECSServiceScalableTarget(ScalableTarget):
    def __init__(self, logicalId, ecscluster, ecsservice, mincap, maxcap, autoscalearn):
        ScalableTarget.__init__(self, logicalId, "", mincap, maxcap, autoscalearn)
        resource_id = self.construct_resource_id(ecscluster, ecsservice)
        self._template["Properties"]["ResourceId"] = resource_id

    def construct_resource_id(self, ecscluster, ecsservice):
        assert isinstance(ecscluster, ECSCluster)
        assert isinstance(ecsservice, ECSService)
        return {"Fn::Join": ["/", ["service", ecscluster.ref(), {"Fn::GetAtt": [ecsservice.logical_id(), "Name"]}]]}

class AASScalingPolicy(Resource):
    def __init__(self, logicalId, name, target):
        assert isinstance(target, ScalableTarget)
        Resource.__init__(self, logicalId)
        self._template = {
            "Type" : "AWS::ApplicationAutoScaling::ScalingPolicy",
            "Properties" : {
                "PolicyName" : name,
                "PolicyType" : "StepScaling",
                "ScalingTargetId" : target.ref(),
                "StepScalingPolicyConfiguration" : {
                    "StepAdjustments" : [ ]
                }
            }
        }

    def cooldown(self, cooldown):
        self._template["Properties"]["StepScalingPolicyConfiguration"]["Cooldown"] = cooldown
        return self

class SimpleAASScalingPolicy(AASScalingPolicy):
    def __init__(self, logicalId, name, target, incremental, lb=None, ub=None):
        AASScalingPolicy.__init__(self, logicalId, name, target)
        self._template["Properties"]["StepScalingPolicyConfiguration"]["AdjustmentType"] = "ChangeInCapacity"
        step = {
            "ScalingAdjustment" : incremental.ref() if isinstance(incremental, Parameter) else str(incremental)
        }
        if lb is not None:
            step["MetricIntervalLowerBound"] = lb
        if ub is not None:
            step["MetricIntervalUpperBound"] = ub
        self._template["Properties"]["StepScalingPolicyConfiguration"]["StepAdjustments"].append(step)


class ExactAASScalingPolicy(AASScalingPolicy):
    def __init__(self, logicalId, name, target, size, lb=None, ub=None):
        AASScalingPolicy.__init__(self, logicalId, name, target)
        self._template["Properties"]["StepScalingPolicyConfiguration"]["AdjustmentType"] = "ExactCapacity"
        step = {
            "ScalingAdjustment" : size.ref() if isinstance(size, Parameter) else str(size)
        }
        if lb is not None:
            step["MetricIntervalLowerBound"] = lb
        if ub is not None:
            step["MetricIntervalUpperBound"] = ub
        self._template["Properties"]["StepScalingPolicyConfiguration"]["StepAdjustments"].append(step)

class PercentAASScalingPolicy(AASScalingPolicy):
    def __init__(self, logicalId, name, target, percent, lb=None, ub=None):
        AASScalingPolicy.__init__(self, logicalId, name, target)
        self._template["Properties"]["StepScalingPolicyConfiguration"]["AdjustmentType"] = "PercentChangeInCapacity"
        step = {
            "ScalingAdjustment" : percent.ref() if isinstance(percent, Parameter) else str(percent)
        }
        if lb is not None:
            step["MetricIntervalLowerBound"] = lb
        elif ub is not None:
            step["MetricIntervalUpperBound"] = ub
        else:
            raise Exception("Must specify either lower bound or upper bound")
        self._template["Properties"]["StepScalingPolicyConfiguration"]["StepAdjustments"].append(step)