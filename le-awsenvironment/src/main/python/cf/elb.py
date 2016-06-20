from .resource import Resource
from .template import Template

class ElasticLoadBalancer(Resource):
    def __init__(self, logicalId):
        Resource.__init__(self, logicalId)

        self.__name = {
            "Fn::Join": [ "-", [
                { "Ref" : "AWS::StackName" },
                { "Ref" : "AWS::Region" },
                logicalId
            ] ] }

        self._template = {
            "Type": "AWS::ElasticLoadBalancing::LoadBalancer",
            "Properties": {
                "Listeners" : [ ],
                "SecurityGroups" : [{ "Ref": "SecurityGroupId" }],
                "Subnets" : [ { "Ref" : "SubnetId1" }, {"Ref": "SubnetId2"} ],
                "LoadBalancerName":  self.__name
            }
        }

    def add_listener(self, listener):
        assert isinstance(listener, ElbListener)
        self._template["Properties"]["Listeners"].append(listener.template())
        return self

    def add_listeners(self, listeners):
        for l in listeners:
            self.add_listener(l)
        return self

    def add_healthcheck(self, healthcheck):
        assert isinstance(healthcheck, ElbHealthCheck)
        self._template["Properties"]["HealthCheck"] = healthcheck.template()
        return self

    def get_name(self):
        return self.__name

class ElbHealthCheck(Template):
    def __init__(self, port, protocal="TCP", path="", healthy_threshold=10, timeout=3, unhealthy_threshold=3, interval=None):
        Template.__init__(self)

        if timeout < 2:
            raise ValueError("timeout has to be >= 2")

        if interval is None:
            interval = (5 * healthy_threshold) * timeout
        self._template = {
            "HealthyThreshold" : healthy_threshold,
            "Interval" : interval,
            "Target" : "%s:%d%s" % (protocal, port, path),
            "Timeout" : timeout,
            "UnhealthyThreshold" : unhealthy_threshold
        }


class ElbListener(Template):
    def __init__(self, port, lb_port=None, protocol="http"):
        Template.__init__(self)

        if lb_port is None:
            lb_port = port

        assert isinstance(int(port), int)
        assert isinstance(int(lb_port), int)

        self._template = {
            "InstancePort" : port,
            "InstanceProtocol" : protocol,
            "LoadBalancerPort" : lb_port,
            "Protocol" : protocol
        }