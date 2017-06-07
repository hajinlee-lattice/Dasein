import boto3
import json
import os
import random
import sys
import tempfile
import time
from boto3.s3.transfer import S3Transfer

from .autoscaling import AutoScalingGroup, LaunchConfiguration, PercentScalingPolicy, ExactScalingPolicy, PercentAASScalingPolicy, ExactAASScalingPolicy, ECSServiceScalableTarget
from .condition import Condition
from .ec2 import EC2Instance, ECSInstance, ecs_metadata
from .ecs import ECSCluster, ECSService
from .parameter import *
from .resource import Resource
from .template import Template, TEMPLATE_DIR
from ...conf import AwsEnvironment

SUBNETS = [PARAM_SUBNET_1, PARAM_SUBNET_2, PARAM_SUBNET_3]

class Stack(Template):
    def __init__(self, description):
        Template.__init__(self)
        self._template["AWSTemplateFormatVersion"] = "2010-09-09"
        self._template["Description"] = description
        self._template["Parameters"] = {}
        self.add_params(COMMON_PARAMETERS)
        self.__mappings()

    def add_ec2(self, instance):
        assert isinstance(instance, EC2Instance)
        name = instance.logical_id()

        # ec2 instance
        data = {
            name: instance.template(),
        }
        self._merge_into_attr('Resources', data)

        # output info
        outputs = {
            name + "Url": {
                "Description" : "URL for EC2 instance " + name,
                "Value" : { "Fn::GetAtt" : [ name, "PrivateDnsName" ]}
            },
            name + "Ip": {
                "Description" : "IP for EC2 instance " + name,
                "Value" : { "Fn::GetAtt" : [ name, "PrivateIp" ]}
            }
        }
        self.add_ouputs(outputs)
        return self

    def add_resource(self, resource):
        assert isinstance(resource, Resource)

        if isinstance(resource, EC2Instance):
            self.add_ec2(resource)
        else:
            self._merge_into_attr("Resources", {resource.logical_id(): resource.template()})

    def add_resources(self, resources):
        for resource in resources:
            self.add_resource(resource)
        return self

    def add_condition(self, condition):
        assert isinstance(condition, Condition)
        self._merge_into_attr("Conditions", condition.template())

    def add_ouputs(self, outputs):
        self._merge_into_attr('Outputs', outputs)
        return self

    def add_params(self, params):
        for param in params:
            self.add_param(param)
        return self

    def add_param(self, param):
        assert isinstance(param, Parameter)
        self._template["Parameters"].update(param.definition())
        return self

    def add_mappings(self, mappings):
        for mapping in mappings:
            self._merge_into_attr("Mappings", mapping)
        return self

    def validate(self):
        print 'Validating template against AWS ...'
        client = boto3.client('cloudformation')
        client.validate_template(
            TemplateBody=self.json()
        )
        print 'Stack template is valid.'

    def upload(self, environment, prefix):

        try:
            bucket = AwsEnvironment(environment).cf_bucket()
            fi, temp_file =   tempfile.mkstemp()
            os.write(fi, self.json())
            os.close(fi)
            print 'uploading template to %s' % (os.path.join("https://", AwsEnvironment(environment).s3_endpoint(), bucket, prefix, 'template.json') + ' ..')
            client = boto3.client('s3')
            transfer = S3Transfer(client)
            transfer.upload_file(temp_file, bucket, os.path.join(prefix, 'template.json'))
            print 'done.'
        finally:
            os.unlink(temp_file)
            try:
                os.close(fi)
            except:
                # assume here because file already closed
                pass


    def __mappings(self):
        json_file = os.path.join(TEMPLATE_DIR, 'common', 'common_mappings.json')
        with open(json_file) as f:
            data = json.load(f)
            data["Environment2Props"] = AwsEnvironment.create_env_props_map()
            self._merge_into_attr('Mappings', data)
            return self

class ECSStack(Stack):
    def __init__(self, description, env, use_asgroup=True, instances=1, efs=None, ips=(), sns_topic=None):
        Stack.__init__(self, description)
        self.add_params(ECS_PARAMETERS)
        self.instances = instances
        if use_asgroup:
            self._ecscluster, self._asgroup = self._construct(efs, sns_topic, env)
        else:
            self._asgroup = None
            self._ecscluster, self._ec2s = self._construct_by_ec2(instances, efs, env, ips=ips)

    def add_service(self, service_name, task, capacity=None, asrolearn=None):
        service, tgt = self.create_service(service_name, task, capacity=capacity, asrolearn=asrolearn)
        self.add_resource(service)
        if tgt is not None:
            self.add_resource(tgt)
        return service, tgt

    def create_service(self, service_name, task, capacity=None, asrolearn=None):
        if capacity is None:
            capacity = PARAM_CAPACITY

        service = ECSService(service_name, self._ecscluster, task, capacity) \
            .set_min_max_percent(50, 200)

        scalable_tgt = None
        if self._asgroup is not None:
            service.depends_on(self._asgroup)
            scalable_tgt = ECSServiceScalableTarget(service.logical_id() + "ScalableTgt", self._ecscluster, service, PARAM_CAPACITY, PARAM_MAX_CAPACITY, autoscalearn=asrolearn)
        else:
            for ec2 in self._ec2s:
                service.depends_on(ec2)

        return service, scalable_tgt

    def exact_autoscale(self, target, policy_name, incremental, cooldown, lb=None, ub=None):
        policy = ExactAASScalingPolicy(target.logical_id() + policy_name, policy_name, target, incremental, lb=lb, ub=ub)
        policy.cooldown(cooldown)
        self.add_resource(policy)
        return policy

    def percent_autoscale(self, target, policy_name, percent, cooldown, lb=None, ub=None):
        policy = PercentAASScalingPolicy(target.logical_id() + policy_name, policy_name, target, percent, lb=lb, ub=ub)
        policy.cooldown(cooldown)
        self.add_resource(policy)
        return policy

    def get_ec2s(self):
        return self._ec2s

    def _construct_by_ec2(self, instances, efs, env, ips=()):
        ecscluster = ECSCluster("ecscluster")
        self.add_resource(ecscluster)
        ec2s = self._create_ec2_instances(ecscluster, instances, efs, env, ips=ips)
        return ecscluster, ec2s

    def _create_ec2_instances(self, ecscluster, instances, efs, env, ips=()):
        ec2s = []
        seed = random.randint(0, 2)
        outputs = {}
        for n in xrange(instances):
            name = "EC2Instance%d" % (n + 1)
            subnet_idx = (seed + n) % 3
            subnet = SUBNETS[subnet_idx]
            ec2 = ECSInstance(name, PARAM_INSTANCE_TYPE, PARAM_KEY_NAME, PARAM_ECS_INSTANCE_PROFILE_NAME, ecscluster, efs, env, PARAM_ECS_INSTANCE_ROLE_NAME) \
                .add_sg(PARAM_SECURITY_GROUP) \
                .set_subnet(subnet) \
                .add_tag("Name", { "Ref" : "AWS::StackName" })

            if subnet_idx < len(ips):
                ip = ips[subnet_idx]
                ec2.set_private_ip(ip)

            ec2s.append(ec2)

            outputs["%sPrivateIp" % name] = {
                "Description" : "Private IP for EC2 instance " + name,
                "Value" : { "Fn::GetAtt" : [ ec2.logical_id(), "PrivateIp" ]}
            }

        self.add_ouputs(outputs)
        self.add_resources(ec2s)
        return ec2s

    def _construct(self, efs, sns_topic, env):
        ecscluster = ECSCluster("ecscluster")
        self.add_resource(ecscluster)
        asgroup = self._create_asgroup(ecscluster, efs, sns_topic, env)
        return ecscluster, asgroup

    def _create_asgroup(self, ecscluster, efs, sns_topic, env):
        asgroup = AutoScalingGroup("scalinggroup", PARAM_CAPACITY, PARAM_CAPACITY, PARAM_MAX_CAPACITY)
        launchconfig = LaunchConfiguration("containerpool").set_instance_profile(PARAM_ECS_INSTANCE_PROFILE_ARN)
        launchconfig.set_metadata(ecs_metadata(launchconfig, ecscluster, efs, env, PARAM_ECS_INSTANCE_ROLE_NAME))
        launchconfig.set_userdata(ECSStack._userdata(launchconfig, asgroup))

        asgroup.add_pool(launchconfig)
        asgroup.attach_tgrp(PARAM_TARGET_GROUP)
        asgroup.add_tag("Name", { "Ref" : "AWS::StackName" }, True)

        if sns_topic is not None:
            asgroup.hook_notifications_to_sns_topic(sns_topic)

        scale_up_policy = PercentScalingPolicy("ScaleUp", asgroup, 100).cooldown(300)
        scale_back_policy = ExactScalingPolicy("ScaleBack", asgroup, PARAM_CAPACITY).cooldown(600)

        self.add_resources([asgroup, launchconfig, scale_up_policy, scale_back_policy])
        return asgroup

    def attach_tgrp(self, tgrp):
        if self._asgroup is not None:
            self._asgroup.attach_tgrp(tgrp)

    @staticmethod
    def _userdata(launch_config, asgroup):
        assert isinstance(launch_config, LaunchConfiguration)
        assert isinstance(asgroup, AutoScalingGroup)
        return { "Fn::Base64" : { "Fn::Join" : ["", [
            "#!/bin/bash -xe\n",
            "yum install -y aws-cfn-bootstrap\n",

            "/opt/aws/bin/cfn-init -v",
            "         -c bootstrap"
            "         --stack ", { "Ref" : "AWS::StackName" },
            "         --resource %s " % launch_config.logical_id(),
            "         --region ", { "Ref" : "AWS::Region" }, "\n",

            "/opt/aws/bin/cfn-signal -e $? ",
            "         --stack ", { "Ref" : "AWS::StackName" },
            "         --resource %s " % asgroup.logical_id(),
            "         --region ", { "Ref" : "AWS::Region" }, "\n"
        ]]}}

    @staticmethod
    def provision(environment, s3cfpath, stackname, security_group, tgrp, init_cap=2, max_cap=8, public=False, additional_params=(), instance_type='t2.medium', le_stack=None):
        #if not elb_not_busy(elb):
        #    return

        config = AwsEnvironment(environment)
        client = boto3.client('cloudformation')
        check_stack_not_exists(client, stackname)

        if public:
            subnet1 = config.public_subnet_1()
            subnet2 = config.public_subnet_2()
            subnet3 = config.public_subnet_3()
        else:
            subnet1 = config.private_subnet_1()
            subnet2 = config.private_subnet_2()
            subnet3 = config.private_subnet_3()

        params = [
            PARAM_VPC_ID.config(config.vpc()),
            PARAM_SUBNET_1.config(subnet1),
            PARAM_SUBNET_2.config(subnet2),
            PARAM_SUBNET_3.config(subnet3),
            PARAM_KEY_NAME.config(config.ec2_key()),
            PARAM_SECURITY_GROUP.config(security_group),
            PARAM_INSTANCE_TYPE.config(instance_type),
            PARAM_ENVIRONMENT.config(environment),
            PARAM_ECS_INSTANCE_PROFILE_ARN.config(config.ecs_instance_profile_arn()),
            PARAM_ECS_INSTANCE_PROFILE_NAME.config(config.ecs_instance_profile_name()),
            PARAM_ECS_INSTANCE_ROLE_NAME.config(config.ecs_instance_role_name()),
            PARAM_TARGET_GROUP.config(tgrp),
            PARAM_CAPACITY.config(str(init_cap)),
            PARAM_MAX_CAPACITY.config(str(max_cap))
        ]

        params += additional_params

        if le_stack is None:
            le_stack = stackname

        response = client.create_stack(
            StackName=stackname,
            TemplateURL='https://%s/%s' % (config.s3_endpoint(), os.path.join(config.cf_bucket(), s3cfpath, 'template.json')),
            Parameters=params,
            TimeoutInMinutes=60,
            OnFailure='ROLLBACK',
            Capabilities=[
                'CAPABILITY_IAM',
            ],
            Tags=[
                {
                    'Key': 'le-env',
                    'Value': config.tag_le_env()
                },
                {
                    'Key': 'le-product',
                    'Value': 'lpi'
                },
                {
                    'Key': 'le-stack',
                    'Value': le_stack
                },
                {
                    'Key': 'le-service',
                    'Value': stackname.replace('-lpi-%s' % le_stack, '')
                }

            ]
        )
        print 'Got StackId: %s' % response['StackId']
        wait_for_stack_creation(client, stackname)

def check_stack_not_exists(client, stackname):
    print 'verifying stack name "%s"...' % stackname
    response = client.list_stacks()
    for stack in response['StackSummaries']:
        if stack['StackStatus'] != 'DELETE_COMPLETE' and stack['StackName'] == stackname:
            raise ValueError('There is already a stack named "%s": %s' % (stackname, stack))
    print 'Great! The name "%s" has not been used.' % stackname
    sys.stdout.flush()

def wait_for_stack_creation(client, stackname):
    print 'Waiting for the stack to be ready ...'
    sys.stdout.flush()
    t1 = time.time()
    waiter = client.get_waiter('stack_create_complete')
    waiter.wait(StackName=stackname)
    t2 = time.time()
    print 'Done. %.2f seconds.' % (t2 -t1)

def teardown_stack(client, stackname):
    client.delete_stack(StackName=stackname)
    wait_for_stack_teardown(client, stackname)

def wait_for_stack_teardown(client, stackname):
    print 'Waiting for the stack to be teardown ...'
    sys.stdout.flush()
    t1 = time.time()
    waiter = client.get_waiter('stack_delete_complete')
    waiter.wait(StackName=stackname)
    t2 = time.time()
    print 'Done. %.2f seconds.' % (t2 -t1)