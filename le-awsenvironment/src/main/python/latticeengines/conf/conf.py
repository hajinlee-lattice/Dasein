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

    def private_subnet(self):
        return self._props['private.subnet.id']

    def zk_sg(self):
        return self._props['zookeeper.sg']

    def kafka_sg(self):
        return self._props['kafka.sg']

    def ecr_registry(self):
        return self._props['ecr.registry']

    def cf_bucket(self):
        return self._props['cf.s3.bucket']

    def ecs_instance_profile(self):
        return self._props['ecs.instance.profile.arn']

    def efs_sg(self):
        return self._props['efs.sg']

    def kafka_create_ecs_role(self):
        return self._props['kafka.create.role'] == 'True'

    def to_props(self):
        return {
            "EcrRegistry": self.ecr_registry()
        }

    @classmethod
    def create_env_props_map(cls):
        map = {}
        for env in ('dev', 'qa', 'prod'):
            map[env] = AwsEnvironment(env).to_props()
        return map