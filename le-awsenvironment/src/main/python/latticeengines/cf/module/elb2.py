from .parameter import *
from .resource import Resource


class TargetGroup(Resource):
    def __init__(self, logicalId, port="80", protocol="http", checkon="/"):
        Resource.__init__(self, logicalId)

        self.__name = {
            "Fn::Join": [ "-", [
                { "Ref" : "AWS::StackName" },
                logicalId
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