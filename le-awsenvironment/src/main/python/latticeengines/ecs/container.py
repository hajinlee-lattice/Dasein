from .template import Template


class Container(Template):
    def __init__(self, name, image):
        Template.__init__(self)
        self._template =  {
            'name': name,
            'image': image
        }

    def mem_mb(self, mem):
        self._template["memory"] = mem

    def cpu(self, cpu):
        self._template["cpu"] = cpu

    def hostname(self, hostname):
        self._template["hostname"] = hostname

    def publish_port(self, container_port, host_port=None, protocol='tcp'):
        if 'portMappings' not in self._template:
            self._template["portMappings"] = []

        if host_port is None:
            self._template["portMappings"].append({
                'containerPort': container_port,
                'protocol': protocol
            })
        else:
            self._template["portMappings"].append({
                'containerPort': container_port,
                'hostPort': host_port,
                'protocol': protocol
            })

    def set_env(self, name, value):
        if 'environment' not in self._template:
            self._template["environment"] = []

        self._template["environment"].append({
            'name': name,
            'value': value
        })

    def log(self, driver, options):
        self._template['logConfiguration'] =   {
            'logDriver': driver,
            'options': options
        }

    def add_docker_label(self, key, value):
        if 'dockerLabels' not in self._template:
            self._template['dockerLabels'] = {}
        self._template['dockerLabels'][key] = value

    def privileged(self):
        self._template["privileged"] = True

    def mount(self, path, source, readonly=False):
        if "mountPoints" not in self._template:
            self._template["mountPoints"] = []
        self._template["mountPoints"].append({
            'sourceVolume': source,
            'containerPath': path,
            'readOnly': readonly
        })
        return self

    def ulimit(self, name, soft, hard):
        limit = {
            'name': name.lower(),
            'softLimit': soft,
            'hardLimit': hard
        }
        if "ulimits" not in self._template:
            self._template["ulimits"] = []
        self._template["ulimits"].append(limit)
        return self

