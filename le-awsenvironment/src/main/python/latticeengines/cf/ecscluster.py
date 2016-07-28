import argparse
import boto3
import httplib
import logging
import os
import sys

from .module.autoscaling import AutoScalingGroup, LaunchConfiguration
from .module.ec2 import EC2Instance
from .module.ecs import ECSCluster
from .module.elb import ElasticLoadBalancer
from .module.iam import InstanceProfile
from .module.parameter import *
from .module.stack import Stack, teardown_stack, check_stack_not_exists, wait_for_stack_creation
from ..conf import AwsEnvironment

_S3_CF_PATH='cloudformation/ecscluster'
_LOG_SIZE=10

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
logging.getLogger('kazoo.client').addHandler(ch)

PARAM_ECS_INSTANCE_PROFILE = ArnParameter("EcsInstanceProfile", "InstanceProfile for ECS instances auto scaling group")
PARAM_APP_PORT = Parameter("ApplicationPort", "Exposed port on each ecs container host", default="8080")
PARAM_CAPACITY = Parameter("DesiredCapacity", "Desired number of containers", type="Number", default="2")
PARAM_MAX_CAPACITY = Parameter("MaximumCapacity", "Desired number of containers", type="Number", default="8")

PARAMS = [PARAM_ECS_INSTANCE_PROFILE, PARAM_APP_PORT, PARAM_CAPACITY, PARAM_MAX_CAPACITY]

def main():
    args = parse_args()
    args.func(args)

def template_cli(args):
    template(args.environment, args.upload, public=args.public)

def template(environment, upload=False, public=False):
    stack = create_template(public=public)

    s3path=_S3_CF_PATH + ("_public" if public else "_private")

    if upload:
        stack.validate()
        stack.upload(environment, s3path)
    else:
        print stack.json()
        stack.validate()

def create_template(public=False):
    stack = Stack("AWS CloudFormation template for ECS cluster.")
    stack.add_params([PARAM_INSTANCE_TYPE, PARAM_SECURITY_GROUP]).add_params(PARAMS)

    # Broker resources
    elb = ElasticLoadBalancer("elb").listen(PARAM_APP_PORT, "80", protocol="http")

    if not public:
        elb.internal()

    ecscluster = ECSCluster("ECSCluster")
    stack.add_resources([elb, ecscluster])

    create_asgroup(stack, ecscluster, elb, PARAM_ECS_INSTANCE_PROFILE)

    # Outputs
    outputs = {
        "ApplicationLoadBalancer": {
            "Description" : "URL for the application load balancer",
            "Value" : { "Fn::GetAtt" : [ elb.logical_id(), "DNSName" ]}
        }
    }
    stack.add_ouputs(outputs)

    return stack

def provision_cli(args):
    return provision(args.environment, args.stackname, args.port, init_cap=args.ic, max_cap=args.mc, consul=args.consul, public=args.public)

def provision(environment, stackname, port=8080, init_cap=2, max_cap=8, consul=None, public=False):
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

    s3path=_S3_CF_PATH + ("_public" if public else "_private")

    response = client.create_stack(
        StackName=stackname,
        TemplateURL='https://s3.amazonaws.com/%s' % os.path.join(config.cf_bucket(), s3path, 'template.json'),
        Parameters=[
            PARAM_VPC_ID.config(config.vpc()),
            PARAM_SUBNET_1.config(subnet1),
            PARAM_SUBNET_2.config(subnet2),
            PARAM_SUBNET_3.config(subnet3),
            PARAM_KEY_NAME.config(config.ec2_key()),
            PARAM_SECURITY_GROUP.config(tomcat_sg),
            PARAM_ENVIRONMENT.config(environment),
            PARAM_ECS_INSTANCE_PROFILE.config(config.ecs_instance_profile()),
            PARAM_APP_PORT.config(port),
            PARAM_CAPACITY.config(str(init_cap)),
            PARAM_MAX_CAPACITY.config(str(max_cap))
        ],
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
    elbs = get_elbs(stackname)

    if consul is not None:
        print 'Saving addresses to consul server %s' % consul
        write_to_consul(consul, "%s/elb" % stackname, elbs["ApplicationLoadBalancer"])
    return elbs

def describe(args):
    stack = boto3.resource('cloudformation').Stack(args.stackname)
    print stack

def teardown_cli(args):
    teardown(args.stackname, consul=args.consul)

def teardown(stackname, consul=None):
    client = boto3.client('cloudformation')
    teardown_stack(client, stackname)
    if consul is not None:
        print 'Removing addresses from consul server %s' % consul
        remove_from_consul(consul, stackname)


def create_asgroup(stack, ecscluster, elb, instance_profile):
    assert isinstance(instance_profile, InstanceProfile) or isinstance(instance_profile, Parameter)

    asgroup = AutoScalingGroup("ScalingGroup", PARAM_CAPACITY, PARAM_CAPACITY, PARAM_MAX_CAPACITY)
    launchconfig = LaunchConfiguration("ContainerPool").set_instance_profile(instance_profile)
    launchconfig.set_metadata(metadata(launchconfig, ecscluster))
    launchconfig.set_userdata(userdata(launchconfig, asgroup))

    asgroup.add_pool(launchconfig)
    asgroup.attach_elbs([elb])
    stack.add_resources([asgroup, launchconfig])


def get_elbs(stackname):
    stack = boto3.resource('cloudformation').Stack(stackname)
    elbs = {}
    for output in stack.outputs:
        key = output['OutputKey']
        value = output['OutputValue']
        if 'LoadBalancer' in key:
            elbs[key] = value
            print "DNS name for %s is %s" % (key, value)
    return elbs

def instance_name(idx):
    return "EC2Instance%d" % (idx + 1)

def userdata(ec2, asgroup):
    assert isinstance(ec2, EC2Instance) or isinstance(ec2, LaunchConfiguration)
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

def metadata(ec2, ecscluster):
    assert isinstance(ec2, EC2Instance) or isinstance(ec2, LaunchConfiguration)
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
                            "echo ECS_RESERVED_PORTS=[22, 5000] >> /etc/ecs/ecs.config\n"
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

def write_to_consul(server, key, value):
    conn = httplib.HTTPConnection(server)
    conn.request("PUT", "/v1/kv/%s" % key, value)
    response = conn.getresponse()
    print response.status, response.reason

def remove_from_consul(server, key):
    conn = httplib.HTTPConnection(server)
    conn.request("DELETE", "/v1/kv/%s?recurse" % key)
    response = conn.getresponse()
    print response.status, response.reason

def parse_args():
    parser = argparse.ArgumentParser(description='Kafka ECS CloudFormation management')
    commands = parser.add_subparsers(help="commands")

    parser1 = commands.add_parser("template")
    parser1.add_argument('-e', dest='environment', type=str, default='qa', choices=['qa','prod'], help='environment')
    parser1.add_argument('-u', dest='upload', action='store_true', help='upload to S3')
    parser1.add_argument('--public', dest='public', action='store_true', help='use public subnets')
    parser1.set_defaults(func=template_cli)

    parser1 = commands.add_parser("provision")
    parser1.add_argument('-e', dest='environment', type=str, default='qa', choices=['qa','prod'], help='environment')
    parser1.add_argument('-s', dest='stackname', type=str, default='ecscluster', help='stack name')
    parser1.add_argument('-p', dest='port', type=str, default='8080', help='application port')
    parser1.add_argument('-c', dest='consul', type=str, help='consul server address')
    parser1.add_argument('--public', dest='public', action='store_true', help='use public subnets')
    parser1.add_argument('--initial-capacity', dest='ic', type=int, default='2', help='initial capacity')
    parser1.add_argument('--max-capacity', dest='mc', type=int, default='8', help='maximum capacity')
    parser1.set_defaults(func=provision_cli)

    parser1 = commands.add_parser("teardown")
    parser1.add_argument('-s', dest='stackname', type=str, default='ecscluster', help='stack name')
    parser1.set_defaults(func=teardown_cli)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
