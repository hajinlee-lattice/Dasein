import os


class AwsEnvironment:
    def __init__(self, env):
        prop_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'env', env, 'aws.env')
        self._props = {}
        with open(prop_file, 'r') as f:
            for line in f:
                if len(line.strip()) > 0 and '#' != line.strip()[0] and '=' in line:
                    k, v = line.split('=')
                    self._props[k] = v.replace('\n', '')

    def aws_account_id(self):
        return self._props['aws.account.id']

    def ec2_key(self):
        return self._props['key.name']

    def vpc(self):
        return self._props['vpc.id']

    def public_subnet_1(self):
        return self._props['public.subnet.id.1']

    def public_subnet_2(self):
        return self._props['public.subnet.id.2']

    def public_subnet_3(self):
        return self._props['public.subnet.id.3']

    def private_subnet_1(self):
        return self._props['private.subnet.id.1']

    def private_subnet_2(self):
        return self._props['private.subnet.id.2']

    def private_subnet_3(self):
        return self._props['private.subnet.id.3']

    def zk_observer_ips(self):
        return self._props['zk.observer.ips'].split(',')

    def zk_sg(self):
        return self._props['zookeeper.sg']

    def kafka_sg(self):
        return self._props['kafka.sg']

    def tomcat_sg(self):
        return self._props['tomcat.sg']

    def tomcat_internal_sg(self):
        return self._props['tomcat.internal.sg']

    def ecr_registry(self):
        return "%s.dkr.ecr.us-east-1.amazonaws.com" % self.aws_account_id()

    def cf_bucket(self):
        return self._props['cf.s3.bucket']

    def ecs_instance_profile_arn(self):
        return "arn:aws:iam::%s:instance-profile/%s" % (self.aws_account_id(), self.ecs_instance_profile_name())

    def ecs_instance_profile_name(self):
        return self._props['ecs.instance.profile.name']

    def efs_sg(self):
        return self._props['efs.sg']

    def kafka_create_ecs_role(self):
        return self._props['kafka.create.role'] == 'True'

    def to_props(self):
        return {
            "EcrRegistry": self.ecr_registry()
        }

    @staticmethod
    def create_env_props_map():
        map = {}
        for env in ('dev', 'qacluster', 'prodcluster'):
            map[env] = AwsEnvironment(env).to_props()
        return map