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

    def aws_region(self):
        return self._props['aws.region']

    def ec2_key(self):
        return self._props['key.name']

    def vpc(self):
        return self._props['vpc.id']

    def tag_le_env(self):
        return self._props['tag.le.env']

    def s3_endpoint(self):
        return self._props['s3.endpoint']

    def subnet_az_1(self):
        return self._props['subnet.az.1']

    def subnet_az_2(self):
        return self._props['subnet.az.2']

    def subnet_az_3(self):
        return self._props['subnet.az.3']

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

    def lpi_efs_id(self):
        return self._props['lpi.efs.id']

    def lpi_efs_ip_1(self):
        return self._props['lpi.efs.ip.1']

    def lpi_efs_ip_2(self):
        return self._props['lpi.efs.ip.2']

    def lpi_efs_ip_3(self):
        return self._props['lpi.efs.ip.3']

    def zk_sg(self):
        return self._props['sg.zookeeper']

    def tomcat_sg(self):
        return self._props['sg.tomcat']

    def ministack_sg(self):
        return self._props['sg.ministack']

    def nodejs_sg(self):
        return self._props['sg.nodejs']

    def ecr_registry(self):
        return "%s.dkr.ecr.%s.amazonaws.com" % (self.aws_account_id(), self.aws_region())

    def ecs_instance_profile_arn(self):
        return "arn:aws:iam::%s:instance-profile/%s" % (self.aws_account_id(), self.ecs_instance_profile_name())

    def ecs_instance_profile_name(self):
        return self._props['ecs.instance.profile.name']

    def ecs_instance_role_name(self):
        return self._props['ecs.instance.role.name']

    def ecs_autoscale_role_arn(self):
        return "arn:aws:iam::%s:role/%s" % (self.aws_account_id(), self._props['ecs.autoscaling.role'])

    def efs_sg(self):
        return self._props['efs.sg']

    def ssl_certificate_arn(self):
        return self._props['ssl.certificate.arn']

    def scaling_sns_topic_arn(self):
        return self._props['scaling.sns.topic.arn']

    def consul_server(self):
        return self._props['consul.server']

    def splunk_url(self):
        return self._props['splunk.url']

    def splunk_token(self):
        return self._props['splunk.token']

    def le_repo(self):
        return self._props['le.repo']

    def cf_bucket(self):
        return self._props['s3.cf.bucket']

    def chef_bucket(self):
        return self._props['s3.chef.bucket']
