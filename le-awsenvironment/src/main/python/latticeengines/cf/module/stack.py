import boto3
import json
import os
import sys
import time
from boto3.s3.transfer import S3Transfer

from .autoscaling import AutoScalingGroup, LaunchConfiguration
from .condition import Condition
from .ec2 import EC2Instance
from .ecs import ECSCluster, ECSService
from .iam import InstanceProfile
from .parameter import *
from .resource import Resource
from .template import Template, TEMPLATE_DIR
from ...conf import AwsEnvironment


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
            # name + "PublicUrl": {
            #     "Description" : "Public URL for EC2 instance " + name,
            #     "Value" : { "Fn::GetAtt" : [ name, "PublicDnsName" ]}
            # },
            # name + "PublicIp": {
            #     "Description" : "Public IP for EC2 instance " + name,
            #     "Value" : { "Fn::GetAtt" : [ name, "PublicIp" ]}
            # },
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
        bucket = AwsEnvironment(environment).cf_bucket()
        temp_file = "/tmp/zookeeper.json"
        with open(temp_file, 'w') as tf:
            tf.write(self.json())
        print 'uploading template to %s' % (os.path.join("https://s3.amazonaws.com", bucket, prefix, 'template.json') + ' ..')
        client = boto3.client('s3')
        transfer = S3Transfer(client)
        transfer.upload_file(temp_file, bucket, os.path.join(prefix, 'template.json'))
        print 'done.'

    def __mappings(self):
        json_file = os.path.join(TEMPLATE_DIR, 'common', 'common_mappings.json')
        with open(json_file) as f:
            data = json.load(f)
            data["Environment2Props"] = AwsEnvironment.create_env_props_map()
            self._merge_into_attr('Mappings', data)
            return self

class ECSStack(Stack):
    def __init__(self, description, extra_elbs=(), use_external_elb=True):
        Stack.__init__(self, description)
        self.add_params(ECS_PARAMETERS)
        self._elbs = [ PARAM_ELB_NAME ] if use_external_elb else []
        self._elbs += list(extra_elbs)
        self._ecscluster, self._asgroup = self._construct(self._elbs)

    def add_service(self, service_name, task, capacity=None):
        if capacity is None:
            capacity = PARAM_CAPACITY

        service = ECSService(service_name, self._ecscluster, task, capacity) \
            .set_min_max_percent(50, 200) \
            .depends_on(self._asgroup)
        self.add_resource(service)

    def _construct(self, elbs):
        ecscluster = ECSCluster("ecscluster")
        self.add_resource(ecscluster)
        asgroup = self._create_asgroup(ecscluster, elbs, PARAM_ECS_INSTANCE_PROFILE)
        return ecscluster, asgroup

    def _create_asgroup(self, ecscluster, elbs, instance_profile):
        assert isinstance(instance_profile, InstanceProfile) or isinstance(instance_profile, Parameter)

        asgroup = AutoScalingGroup("scalinggroup", PARAM_CAPACITY, PARAM_CAPACITY, PARAM_MAX_CAPACITY)
        launchconfig = LaunchConfiguration("containerpool").set_instance_profile(instance_profile)
        launchconfig.set_metadata(ECSStack._metadata(launchconfig, ecscluster))
        launchconfig.set_userdata(ECSStack._userdata(launchconfig, asgroup))

        asgroup.add_pool(launchconfig)
        asgroup.attach_elbs(elbs)
        self.add_resources([asgroup, launchconfig])
        return asgroup

    @staticmethod
    def _userdata(ec2, asgroup):
        assert isinstance(ec2, LaunchConfiguration)
        assert isinstance(asgroup, AutoScalingGroup)
        return { "Fn::Base64" : { "Fn::Join" : ["", [
            "#!/bin/bash -xe\n",
            "yum install -y aws-cfn-bootstrap\n",

            "/opt/aws/bin/cfn-init -v",
            "         -c bootstrap"
            "         --stack ", { "Ref" : "AWS::StackName" },
            "         --resource %s " % ec2.logical_id(),
            "         --region ", { "Ref" : "AWS::Region" }, "\n",

            "/opt/aws/bin/cfn-signal -e $? ",
            "         --stack ", { "Ref" : "AWS::StackName" },
            "         --resource %s " % asgroup.logical_id(),
            "         --region ", { "Ref" : "AWS::Region" }, "\n"
        ]]}}

    @staticmethod
    def _metadata(ec2, ecscluster):
        assert isinstance(ec2, LaunchConfiguration)
        assert isinstance(ecscluster, ECSCluster)
        return {
            "AWS::CloudFormation::Init" : {
                "configSets": {
                    "bootstrap": [ "install" ],
                    "reload": [ "install" ]
                },
                "install" : {
                    "files" : {
                        "/etc/cfn/cfn-hup.conf" : {
                            "content" : { "Fn::Join" : ["", [
                                "[main]\n",
                                "stack=", { "Ref" : "AWS::StackId" }, "\n",
                                "region=", { "Ref" : "AWS::Region" }, "\n"
                            ]]},
                            "mode"    : "000400",
                            "owner"   : "root",
                            "group"   : "root"
                        },
                        "/etc/cfn/hooks.d/cfn-auto-reloader.conf" : {
                            "content": { "Fn::Join" : ["", [
                                "[cfn-auto-reloader-hook]\n",
                                "triggers=post.update\n",
                                "path=Resources.ContainerInstances.Metadata.AWS::CloudFormation::Init\n",
                                "action=/opt/aws/bin/cfn-init -v",
                                "         -c reload",
                                "         --stack ", { "Ref" : "AWS::StackName" },
                                "         --resource %s " % ec2.logical_id(),
                                "         --region ", { "Ref" : "AWS::Region" }, "\n",
                                "runas=root\n"
                            ]]}
                        }
                    },
                    "commands" : {
                        "01_save_ip" : {
                            "command" : { "Fn::Join": [ "\n", [
                                "#!/bin/bash",
                                "ADDR=`ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'`",
                                "echo $ADDR >> /etc/internaladdr.txt",
                                "PUBADDR=`curl http://169.254.169.254/latest/meta-data/public-ipv4`",
                                "echo $PUBADDR >> /etc/externaladdr.txt"
                            ] ] }
                        },
                        "02_add_instance_to_cluster" : {
                            "command" : { "Fn::Join": [ "", [
                                "#!/bin/bash\n",
                                "rm -rf /etc/ecs/ecs.config\n",
                                "touch /etc/ecs/ecs.config\n",
                                "echo ECS_CLUSTER=", ecscluster.ref(), " >> /etc/ecs/ecs.config\n",
                                "echo ECS_AVAILABLE_LOGGING_DRIVERS=[\\\"json-file\\\", \\\"awslogs\\\"] >> /etc/ecs/ecs.config\n",
                                "echo ECS_RESERVED_PORTS=[22] >> /etc/ecs/ecs.config\n"
                            ] ] }
                        }
                    },
                    "services" : {
                        "sysvinit" : {
                            "cfn-hup" : { "enabled" : "true", "ensureRunning" : "true", "files" : ["/etc/cfn/cfn-hup.conf", "/etc/cfn/hooks.d/cfn-auto-reloader.conf"] }
                        }
                    }
                }
            }
        }

    @staticmethod
    def provision(environment, s3cfpath, stackname, elb, init_cap=2, max_cap=8, public=False, additional_params=(), instance_type='t2.medium'):
        if not elb_not_busy(elb):
            return

        config = AwsEnvironment(environment)
        client = boto3.client('cloudformation')
        check_stack_not_exists(client, stackname)

        if public:
            subnet1 = config.public_subnet_1()
            subnet2 = config.public_subnet_2()
            subnet3 = config.public_subnet_3()
            tomcat_sg = config.tomcat_sg()
        else:
            subnet1 = config.private_subnet_1()
            subnet2 = config.private_subnet_2()
            subnet3 = config.private_subnet_3()
            tomcat_sg = config.tomcat_internal_sg()

        params = [
            PARAM_VPC_ID.config(config.vpc()),
            PARAM_SUBNET_1.config(subnet1),
            PARAM_SUBNET_2.config(subnet2),
            PARAM_SUBNET_3.config(subnet3),
            PARAM_KEY_NAME.config(config.ec2_key()),
            PARAM_SECURITY_GROUP.config(tomcat_sg),
            PARAM_INSTANCE_TYPE.config(instance_type),
            PARAM_ENVIRONMENT.config(environment),
            PARAM_ECS_INSTANCE_PROFILE.config(config.ecs_instance_profile()),
            PARAM_ELB_NAME.config(elb),
            PARAM_CAPACITY.config(str(init_cap)),
            PARAM_MAX_CAPACITY.config(str(max_cap))
        ]

        params += additional_params

        response = client.create_stack(
            StackName=stackname,
            TemplateURL='https://s3.amazonaws.com/%s' % os.path.join(config.cf_bucket(), s3cfpath, 'template.json'),
            Parameters=params,
            TimeoutInMinutes=60,
            OnFailure='ROLLBACK',
            Capabilities=[
                'CAPABILITY_IAM',
            ],
            Tags=[
                {
                    'Key': 'com.lattice-engines.cluster.name',
                    'Value': stackname
                },
                {
                    'Key': 'com.lattice-engines.cluster.type',
                    'Value': 'ecs'
                },
            ]
        )
        print 'Got StackId: %s' % response['StackId']
        wait_for_stack_creation(client, stackname)
        toggle_healtch_check(elb)

def toggle_healtch_check(elb):
    """
    sometimes when re-associate an elb to a new autoscaling group, it cannot find health instances.
    need to toggle health check between tcp and http once or twice to make it work
    :param elb:
    :return:
    """
    client = boto3.client('elb')
    desc = client.describe_load_balancers(LoadBalancerNames=[elb])['LoadBalancerDescriptions'][0]
    hc = desc['HealthCheck']

    print 'change health check of %s to tcp:80, and wait for 5 seconds.' % elb
    client.configure_health_check(
        LoadBalancerName=elb,
        HealthCheck={
            'Target': 'TCP:80',
            'Interval': 5,
            'Timeout': 30,
            'UnhealthyThreshold': 2,
            'HealthyThreshold': 10
        }
    )

    time.sleep(5)

    print 'resume health check of %s to its original setting:\n%s' % (elb, json.dumps(hc, indent=2, separators=(',', ': ')))
    client.configure_health_check(
        LoadBalancerName=elb,
        HealthCheck=hc
    )

def elb_not_busy(elb):
    elbs = boto3.client('elb').describe_load_balancers(LoadBalancerNames=[elb])
    desc = elbs['LoadBalancerDescriptions'][0]
    ins = desc['Instances']
    if ins is not None and len(ins) > 0:
        print "Load Balancer " + elb + " is busy:\n" + ins + "\nCannot use this elb for the stack to be provisioned"
        return False
    return True

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