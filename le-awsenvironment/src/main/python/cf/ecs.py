from .elb import ElasticLoadBalancer
from .iam import Role
from .resource import Resource
from .template import Template


class ECSCluster(Resource):
    def __init__(self, logicalId):
        Resource.__init__(self, logicalId)
        self._template = {
            "Type": "AWS::ECS::Cluster"
        }

class ECSService(Resource):
    def __init__(self, logicalId, ecscluster, task, role, init_count):

        assert isinstance(ecscluster, ECSCluster)
        assert isinstance(task, TaskDefinition)
        assert isinstance(role, Role)

        Resource.__init__(self, logicalId)
        self._template = {
            "Type" : "AWS::ECS::Service",
            "Properties" : {
                "Cluster" : ecscluster.ref(),
                "DesiredCount" : init_count,
                "Role" : role.ref(),
                "TaskDefinition" : task.ref()
            }
        }

    def add_elb(self, elb, container, port):
        assert isinstance(elb, ElasticLoadBalancer)
        assert isinstance(container, ContainerDefinition)

        if "LoadBalancers" not in self._template["Properties"]:
            self._template["Properties"]["LoadBalancers"] = []

        ecs_lb = ECSLoadBalancer(container.get_name(), port, elb.get_name())
        self._template["Properties"]["LoadBalancers"].append(ecs_lb.template())

        return self

class ECSLoadBalancer(Template):
    def __init__(self, container, port, lb):
        Template.__init__(self)
        self._template = {
            "ContainerName" : container,
            "ContainerPort" : port,
            "LoadBalancerName" : lb
        }


class TaskDefinition(Resource):
    def __init__(self, logicalId):
        Resource.__init__(self, logicalId)
        self._template = {
            "Type": "AWS::ECS::TaskDefinition",
            "Properties" : {
                "ContainerDefinitions" : []
            }
        }

    def add_container(self, container):
        assert isinstance(container, ContainerDefinition)
        self._template["Properties"]["ContainerDefinitions"].append(container.template())
        return self

    def add_containers(self, containers):
        for c in containers:
            self.add_container(c)
        return self


class ContainerDefinition(Template):
    def __init__(self, name, image):
        Template.__init__(self)
        self._template = {
            "Name": name,
            "Cpu": "128",
            "Essential": "true",
            "Image": image,
            "Memory":"256"
        }

    def get_name(self):
        return self._template["Name"]

    def cpu(self, cpu_unit):
        # 1 ECU = 1024 cpu unit
        self._template["Cpu"] = cpu_unit
        return self

    def mem_mb(self, mem):
        self._template["Memory"] = mem
        return self

    def set_essential(self, essential):
        assert isinstance(essential, bool)
        self._template["Essential"] = str(essential).lower()
        return self

    def publish_port(self, lb_port, container_port):
        assert isinstance(int(container_port), int)
        assert isinstance(int(lb_port), int)

        if "PortMappings" not in self._template:
            self._template["PortMappings"] = []

        self._template["PortMappings"].append({
            "HostPort": lb_port,
            "ContainerPort": container_port
        })
        return self