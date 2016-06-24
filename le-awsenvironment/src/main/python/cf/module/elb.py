from .resource import Resource
from .template import Template
from .ec2 import EC2Instance

class ElasticLoadBalancer(Resource):
    def __init__(self, logicalId):
        Resource.__init__(self, logicalId)

        self.__name = {
            "Fn::Join": [ "-", [
                { "Ref" : "AWS::StackName" },
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

    def add_instance(self, instance):
        assert isinstance(instance, EC2Instance)
        if "Instance" not in self._template["Properties"]:
            self._template["Properties"]["Instances"] = []
        self._template["Properties"]["Instances"].append(instance.template())
        return self

    def add_listener(self, listener):
        assert isinstance(listener, ElbListener)
        self._template["Properties"]["Listeners"].append(listener.template())
        return self

    def listen(self, port, protocol="tcp"):
        if protocol in ("http", "https"):
            return self.add_listener(ElbListener(port, protocol=protocol))
        else:
            return self.add_listener(ElbListener(port, protocol=protocol))

    def add_listeners(self, listeners):
        for l in listeners:
            self.add_listener(l)
        return self

    def add_healthcheck(self, healthcheck):
        assert isinstance(healthcheck, ElbHealthCheck)
        self._template["Properties"]["HealthCheck"] = healthcheck.template()
        return self

    def add_policy(self, policy):
        assert isinstance(policy, ElbPolicy)
        if "Policies" not in self._template["Properties"]:
            self._template["Properties"]["Policies"] = []
        self._template["Properties"]["Policies"].append(policy.template())
        return self

    def proxy(self, port):
        return self.add_policy(ProxyPolicy(port))

    def get_name(self):
        return self.__name

class ElbPolicy(Template):
    def __init__(self, name):
        Template.__init__(self)
        self._template = {
            "Attributes" : [ ],
            "PolicyName" : name
        }

    def add_attr(self, name, value):
        self._template["Attributes"].append({
            "Name": name,
            "Value": value
        })
        return self

    def add_instance_port(self, port):
        if "InstancePorts" not in self._template:
            self._template["InstancePorts"] = []
        self._template["InstancePorts"].append(port)
        return self

class ProxyPolicy(ElbPolicy):
    def __init__(self, port):
        ElbPolicy.__init__(self, "EnableProxyProtocol")
        self._template["PolicyType"] = "ProxyProtocolPolicyType"
        self.add_attr("ProxyProtocol", "true")
        self.add_instance_port(port)

class ElbHealthCheck(Template):
    def __init__(self, port, protocol="TCP", path=None, healthy_threshold=3, timeout=3, unhealthy_threshold=10, interval=None):
        Template.__init__(self)

        if path is None:
            path = "/" if protocol in ("HTTP", "HTTPS") else ""

        if timeout < 2:
            raise ValueError("timeout has to be >= 2")

        if interval is None:
            interval = min((2 * healthy_threshold) * timeout, 60)
        self._template = {
            "HealthyThreshold" : healthy_threshold,
            "Interval" : interval,
            "Target" : "%s:%s%s" % (protocol, port, path),
            "Timeout" : timeout,
            "UnhealthyThreshold" : unhealthy_threshold
        }

class ElbListener(Template):
    def __init__(self, port, lb_port=None, protocol="tcp"):
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