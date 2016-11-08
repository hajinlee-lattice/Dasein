from .parameter import *
from .resource import Resource


class TargetGroup(Resource):
    def __init__(self, logicalId, port="80", protocol="http", checkon="/"):
        Resource.__init__(self, logicalId)

        self.__name = {
            "Fn::Join": [ "-", [
                logicalId,
                { "Ref" : "AWS::StackName" }
            ] ] }

        self._template = {
            "Type": "AWS::ElasticLoadBalancingV2::TargetGroup",
            "Properties" : {
                "HealthCheckPath" : checkon,
                "Name" : self.__name,
                "Port" : port,
                "Protocol" : protocol,
                "VpcId" : PARAM_VPC_ID.ref()
            }
        }

class Listener(Resource):
    def __init__(self, logicalId, lb, tg, port=443, protocol="HTTPS"):
        assert isinstance(lb, ApplicationLoadBalancer)
        Resource.__init__(self, logicalId)
        self._template = {
            "Type" : "AWS::ElasticLoadBalancingV2::Listener",
            "Properties" : {
                "Certificates" : [ {
                    "CertificateArn" : PARAM_SSL_CERTIFICATE_ARN.ref()
                } ],
                "DefaultActions" : [ {
                    "TargetGroupArn": tg.ref(),
                    "Type": "forward"
                } ],
                "LoadBalancerArn" : lb.ref(),
                "Port" : port,
                "Protocol" : protocol
            }
        }

class ListenerRule(Resource):
    def __init__(self, logicalId, listener, priority, tg, path_patterns):
        assert isinstance(listener, Listener)
        assert isinstance(tg, TargetGroup)
        Resource.__init__(self, logicalId)
        self._template = {
            "Type" : "AWS::ElasticLoadBalancingV2::ListenerRule",
            "Properties" : {
                "Actions" : [ {
                    "TargetGroupArn" : tg.ref(),
                    "Type" : "forward"
                } ],
                "Conditions" : [ {
                    "Field": "path-pattern",
                    "Values": [ path_patterns ]
                } ],
                "ListenerArn" : listener.ref(),
                "Priority" : priority
            }
        }

class ApplicationLoadBalancer(Resource):
    def __init__(self, name, sg, subnet_params):
        logicalId = name + "Alb"
        Resource.__init__(self, logicalId)
        self.__name = name
        self._template = {
            "Type" : "AWS::ElasticLoadBalancingV2::LoadBalancer",
            "Properties" : {
                "Name" : {
                    "Fn::Join": [ "-", [
                        name,
                        { "Ref" : "AWS::StackName" }
                    ] ] },
                "Scheme" : "internal",
                "SecurityGroups" : [ sg.ref() ],
                "Subnets" : [ p.ref() for p in subnet_params ]
            }
        }

    def name(self):
        return self.__name
