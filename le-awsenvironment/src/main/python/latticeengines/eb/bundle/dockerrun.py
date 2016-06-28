
import json

class Template:
    def __init__(self):
        self._template =  {}

    def _merge_into_attr(self, attr, data):
        self._initialize_attr(attr)
        for k, v in data.items():
            self._template[attr][k] = v

    def _initialize_attr(self, attr):
        if attr not in self._template:
            self._template[attr] = {}

    def template(self):
        return self._template

    def json(self):
        return json.dumps(self._template, indent=2, separators=(',', ': '))


class DockerRun(Template):
    def __init__(self):
        Template.__init__(self)

    @classmethod
    def filename(cls):
        return "Dockerrun.aws.json"

class SingleDockerRun(DockerRun):
    def __init__(self, image):
        Template.__init__(self)
        self._template = {
            "AWSEBDockerrunVersion": "1",
            "Image": {
                "Name": image,
                "Update": "true"
            },
            "Ports": [
                {
                    "ContainerPort": "1234"
                }
            ],
            "Volumes": [
                {
                    "HostDirectory": "/var/app/mydb",
                    "ContainerDirectory": "/etc/mysql"
                }
            ],
            "Logging": "/var/log/nginx"
        }

class MultiDockerRun(DockerRun):
    def __init__(self):
        Template.__init__(self)
        self._template = {
            "AWSEBDockerrunVersion": 2,
            "containerDefinitions": []
        }

    def add_container(self, container):
        assert isinstance(container, Container)
        self._template["containerDefinitions"].append(container.template())
        return self

class Container(Template):
    def __init__(self, name, image, memory, essential=True):
        Template.__init__(self)
        self._template = {
            "name": name,
            "image": image,
            "essential": essential,
            "memory": memory
        }

    def add_env(self, name, value):
        if "environment" not in self._template:
            self._template["environment"] = []
        self._template["environment"].append({
            "name": name,
            "value": value
        })
        return self

    def mount(self, path, volume, read_only=True):
        if "mountPoints" not in self._template:
            self._template["mountPoints"] = []
        self._template["mountPoints"].append({
            "sourceVolume": volume,
            "containerPath": path,
            "readOnly": read_only
        })
        return self

    def publish_port(self, container_port, host_port):
        if "portMappings" not in self._template:
            self._template["portMappings"] = []

        if host_port is None:
            self._template["portMappings"].append({
                "containerPort": container_port
            })
        else:
            self._template["portMappings"].append({
                "hostPort": host_port,
                "containerPort": container_port
            })
        return self