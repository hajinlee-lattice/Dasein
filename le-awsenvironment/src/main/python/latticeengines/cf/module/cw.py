from .autoscaling import AutoScalingGroup, ScalingPolicy
from .ecs import ECSCluster, ECSService
from .elb2 import TargetGroup
from .resource import Resource


class CloudWatchAlarm(Resource):
    def __init__(self, logicalId, name, namespace, metric):
        Resource.__init__(self, logicalId)
        self._template = {
            "Type" : "AWS::CloudWatch::Alarm",
            "Properties": {
                "AlarmName": name,
                "AlarmActions": [ ],
                "Namespace": namespace,
                "Dimensions": [ ],
                "MetricName": metric
            }
        }

    def evaluate(self, operator, threshold, period_minute=1, eval_periods=3, stat="Average"):
        assert isinstance(period_minute, int)
        self._template["Properties"]["ComparisonOperator"] = operator
        self._template["Properties"]["Threshold"] = str(threshold)
        self._template["Properties"]["Period"] = str(period_minute * 60)
        self._template["Properties"]["EvaluationPeriods"] = eval_periods
        self._template["Properties"]["Statistic"] = stat
        return self

    def add_targetgroup(self, elb, targetgroup):
        assert isinstance(targetgroup, TargetGroup)
        self._template["Properties"]["Dimensions"].append({
            "Name": "TargetGroup",
            "Value": targetgroup.ref()
        })
        self._template["Properties"]["Dimensions"].append({
            "Name": "LoadBalancer",
            "Value": elb.ref()
        })
        return self

    def add_asgroup(self, asgroup):
        assert isinstance(asgroup, AutoScalingGroup)
        self._template["Properties"]["Dimensions"].append({
            "Name": "AutoScalingGroupName",
            "Value": asgroup.ref()
        })
        return self


    def add_ecscluster(self, ecscluster):
        assert isinstance(ecscluster, ECSCluster)
        self._template["Properties"]["Dimensions"].append({
            "Name": "ClusterName",
            "Value": ecscluster.ref()
        })
        return self

    def add_ecsservice(self, ecscluster, ecsservice):
        assert isinstance(ecsservice, ECSService)
        self.add_ecscluster(ecscluster)
        self._template["Properties"]["Dimensions"].append({
            "Name": "ServiceName",
            "Value": ecsservice.ref()
        })
        return self

    def add_scaling_policy(self, scaling):
        assert isinstance(scaling, ScalingPolicy)
        self._template["Properties"]["AlarmActions"].append(scaling.ref())
        return self