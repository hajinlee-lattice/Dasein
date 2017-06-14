from .parameter import *
from .resource import Resource


class TargetGroup(Resource):
    def __init__(self, logicalId, port="80", protocol="http", checkon="/", name=None):
        Resource.__init__(self, logicalId)

        if name is None:
            self.__name = {
                "Fn::Join": ["-", [
                    logicalId,
                    {"Ref": "AWS::StackName"}
                ]]}
        else:
            self.__name = name

        self._template = {
            "Type": "AWS::ElasticLoadBalancingV2::TargetGroup",
            "Properties": {
                "HealthCheckPath": checkon,
                "Name": self.__name,
                "Port": port,
                "Protocol": protocol,
                "VpcId": PARAM_VPC_ID.ref()
            }
        }


class Listener(Resource):
    def __init__(self, logicalId, lb, tg, port=443, protocol="HTTPS"):
        assert isinstance(lb, ApplicationLoadBalancer)
        Resource.__init__(self, logicalId)
        self._template = {
            "Type": "AWS::ElasticLoadBalancingV2::Listener",
            "Properties": {
                "Certificates": [{
                    "CertificateArn": PARAM_SSL_CERTIFICATE_ARN.ref()
                }],
                "DefaultActions": [{
                    "TargetGroupArn": tg.ref(),
                    "Type": "forward"
                }],
                "LoadBalancerArn": lb.ref(),
                "Port": port,
                "Protocol": protocol
            }
        }


class ListenerRule(Resource):
    def __init__(self, logicalId, listener, priority, tg, path_patterns):
        assert isinstance(listener, Listener)
        assert isinstance(tg, TargetGroup)
        Resource.__init__(self, logicalId)
        self._template = {
            "Type": "AWS::ElasticLoadBalancingV2::ListenerRule",
            "Properties": {
                "Actions": [{
                    "TargetGroupArn": tg.ref(),
                    "Type": "forward"
                }],
                "Conditions": [{
                    "Field": "path-pattern",
                    "Values": [path_patterns]
                }],
                "ListenerArn": listener.ref(),
                "Priority": priority
            }
        }


class ApplicationLoadBalancer(Resource):
    def __init__(self, name, sg, subnet_params, internet_facing=False):
        logicalId = name + "Alb"
        Resource.__init__(self, logicalId)
        self.__name = name
        self._template = {
            "Type": "AWS::ElasticLoadBalancingV2::LoadBalancer",
            "Properties": {
                "Name": {
                    "Fn::Join": ["-", [
                        {"Ref": "AWS::StackName"},
                        name
                    ]]},
                "Scheme": "internet_facing" if internet_facing else "internal",
                "SecurityGroups": [sg.ref()],
                "Subnets": [p.ref() for p in subnet_params]
            }
        }

    def name(self):
        return self.__name

    def idle_timeout(self, second):
        if "LoadBalancerAttributes" not in self._template["Properties"]:
            self._template["Properties"]["LoadBalancerAttributes"] = []
        for kv in self._template["Properties"]["LoadBalancerAttributes"]:
            if kv["Key"] == "idle_timeout.timeout_seconds":
                kv["Value"] = str(second)
                return self
        self._template["Properties"]["LoadBalancerAttributes"].append(
            {"Key": "idle_timeout.timeout_seconds", "Value": str(second)})
        return self
