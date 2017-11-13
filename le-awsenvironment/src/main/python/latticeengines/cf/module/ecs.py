from .elb import ElasticLoadBalancer
from .iam import Role
from .parameter import Parameter
from .resource import Resource
from .template import Template


class ECSCluster(Resource):
    def __init__(self, logicalId):
        Resource.__init__(self, logicalId)
        self._template = {
            "Type": "AWS::ECS::Cluster"
        }

class ECSService(Resource):
    def __init__(self, logicalId, ecscluster, task, init_count):

        assert isinstance(ecscluster, ECSCluster) or isinstance(ecscluster, Parameter)
        assert isinstance(task, TaskDefinition)

        assert isinstance(init_count, int) or isinstance(init_count, Parameter)

        Resource.__init__(self, logicalId)
        self._template = {
            "Type" : "AWS::ECS::Service",
            "Properties" : {
                "Cluster" : ecscluster.ref(),
                "DesiredCount" : init_count,
                "TaskDefinition" : task.ref()
            }
        }

        if isinstance(init_count, Parameter):
            self._template['Properties']["DesiredCount"] = init_count.ref()


    def set_min_max_percent(self, min_pct, max_pct):
        self._template["Properties"]["DeploymentConfiguration"] = {
            "MaximumPercent" : max_pct,
            "MinimumHealthyPercent" : min_pct
        }
        return self

    def add_elb(self, elb, role, container, port):
        assert isinstance(elb, ElasticLoadBalancer)
        assert isinstance(container, ContainerDefinition)
        assert isinstance(role, Role)

        if "LoadBalancers" not in self._template["Properties"]:
            self._template["Properties"]["LoadBalancers"] = []

        ecs_lb = ECSLoadBalancer(container.get_name(), port, elb.get_name())
        self._template["Properties"]["LoadBalancers"].append(ecs_lb.template())
        self._template["Properties"]["Role"] = role.ref()

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

    def add_volume(self, volume):
        assert isinstance(volume, Volume)
        if "Volumes" not in self._template["Properties"]:
            self._template["Properties"]["Volumes"] = []
        self._template["Properties"]["Volumes"].append(volume.template())
        return self


class ContainerDefinition(Template):
    def __init__(self, name, image):
        Template.__init__(self)
        self._template = {
            "Name": name,
            "Essential": "true",
            "Image": image,
            "Memory":"256"
        }

    def get_name(self):
        return self._template["Name"]

    def hostname(self, hostname):
        self._template["Hostname"] = hostname
        return self

    def command(self, cmds):
        self._template["Command"] = cmds
        return self

    def link(self, container, alias=None):
        assert isinstance(container, ContainerDefinition)
        if "Links" not in self._template:
            self._template["Links"] = []
        if alias is None:
            self._template["Links"].append(container.get_name())
        else:
            self._template["Links"].append("%s:%s" % (container.get_name(), alias))
        return self

    def privileged(self):
        self._template["Privileged"] = "true"
        return self

    def cpu(self, cpu_unit):
        # 1 core = 1024 cpu unit
        self._template["Cpu"] = cpu_unit
        return self

    def mem_mb(self, mem):
        self._template["Memory"] = mem
        return self

    def set_env(self, name, value):
        if "Environment" not in self._template:
            self._template["Environment"] = []
        self._template["Environment"].append({
            "Name": name,
            "Value": value
        })
        return self

    def set_logging(self, logging):
        self._template["LogConfiguration"] = logging
        return self

    def set_essential(self, essential):
        assert isinstance(essential, bool)
        self._template["Essential"] = str(essential).lower()
        return self

    def add_docker_label(self, key, value):
        if "DockerLabels" not in self._template:
            self._template["DockerLabels"] = {}
        self._template["DockerLabels"][key] = value
        return self

    def publish_port(self, container_port, lb_port=None):
        assert isinstance(int(container_port), int)

        if "PortMappings" not in self._template:
            self._template["PortMappings"] = []

        if lb_port is None:
            self._template["PortMappings"].append({
                "ContainerPort": container_port
            })
        if lb_port is not None:
            assert isinstance(int(lb_port), int)

            self._template["PortMappings"].append({
                "HostPort": lb_port,
                "ContainerPort": container_port
            })

        return self

    def mount(self, path, source):
        mount_point = MountPoint(path, source)
        if "MountPoints" not in self._template:
            self._template["MountPoints"] = []
        self._template["MountPoints"].append(mount_point.template())
        return self

    def ulimit(self, name, soft, hard):
        limit = {
            "Name": name.upper(),
            "SoftLimit": soft,
            "HardLimit": hard
        }
        if "Ulimits" not in self._template:
            self._template["Ulimits"] = []
        self._template["Ulimits"].append(limit)
        return self


class Volume(Template):
    def __init__(self, name, host_path):
        Template.__init__(self)
        self._template = {
            "Name" : name,
            "Host" : {
                "SourcePath" : host_path
            }
        }

    def get_name(self):
        return self._template["Name"]


class MountPoint(Template):
    def __init__(self, path, source, readonly=False):
        assert isinstance(source, Volume)
        Template.__init__(self)
        self._template = {
            "ContainerPath" : path,
            "SourceVolume" : source.get_name(),
            "ReadOnly" : readonly
        }





















