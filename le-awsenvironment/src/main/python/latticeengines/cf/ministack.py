"""
ECS stack for zookeeper
"""

import argparse
import boto3
import json
import os

from .consul import write_to_stack
from .module.ecs import ContainerDefinition, TaskDefinition
from .module.parameter import *
from .module.stack import ECSStack, teardown_stack, check_stack_not_exists, wait_for_stack_creation
from .module.template import TEMPLATE_DIR
from ..conf import AwsEnvironment
from ..cw.logs import create_internal as create_log_group
from ..elb.targetgroup import DUMMY_TGRP

_S3_CF_PATH='cloudformation/swagger'

PARAM_SWAGGER_APPS=Parameter("SwaggerApps", "List of apps for swagger.")
PROFILE = {}

def main():
    args = parse_args()
    args.func(args)

def template_cli(args):
    template(args.environment, args.instances, upload=args.upload)

def template(environment, instances, upload=False):
    stack = create_template(instances)
    if upload:
        stack.validate()
        stack.upload(environment, _S3_CF_PATH)
    else:
        print stack.json()
        stack.validate()

def create_template(instances):
    global PROFILE
    PROFILE = load_profile()

    stack = ECSStack("AWS CloudFormation template for mini-stack ECS cluster.", use_asgroup=False, instances=instances)
    stack.add_param(PARAM_SWAGGER_APPS)

    task = swagger_task()
    stack.add_resource(task)
    stack.add_service("swagger", task, capacity=1)

    task = haproxy_task(stack.get_ec2s())
    stack.add_resource(task)
    stack.add_service("haproxy", task, capacity=1)

    return stack

def swagger_task():
    profile = PROFILE["swagger"]

    container = ContainerDefinition("httpd", { "Fn::Join" : [ "", [
        { "Fn::FindInMap" : [ "Environment2Props", PARAM_ENVIRONMENT.ref(), "EcrRegistry" ] },
        "/latticeengines/swagger" ] ]}) \
        .mem_mb("%d" % profile["mem"]) \
        .publish_port(80, profile["port"]) \
        .set_logging({
        "LogDriver": "awslogs",
        "Options": {
            "awslogs-group": { "Fn::Join" : ["", ["docker-ministack-", { "Ref" : "AWS::StackName" }]]},
            "awslogs-region": { "Ref": "AWS::Region" }
        }}) \
        .set_env("SWAGGER_APPS", PARAM_SWAGGER_APPS.ref()) \
        .set_env("JVMFLAGS", "-Xms%dm -Xmx%dm" % (profile["xms"], profile["xmx"]))
    task = TaskDefinition("swaggertask")
    task.add_container(container)
    return task


def haproxy_task(ec2s):
    profile = PROFILE["haproxy"]

    tokens = []
    for ec2 in ec2s:
        tokens.append({ "Fn::GetAtt" : [ ec2.logical_id(), "PrivateIp" ]})
    ips = { "Fn::Join" : [ ",", tokens ]}

    container = ContainerDefinition("haproxy", { "Fn::Join" : [ "", [
        { "Fn::FindInMap" : [ "Environment2Props", PARAM_ENVIRONMENT.ref(), "EcrRegistry" ] },
        "/latticeengines/haproxy" ] ]}) \
        .mem_mb("%d" % profile["mem"]) \
        .publish_port(80, profile["port"]) \
        .set_logging({
        "LogDriver": "awslogs",
        "Options": {
            "awslogs-group": { "Fn::Join" : ["", ["docker-ministack-", { "Ref" : "AWS::StackName" }]]},
            "awslogs-region": { "Ref": "AWS::Region" }
        }}) \
        .set_env("HOSTS", ips) \
        .set_env("JVMFLAGS", "-Xms%dm -Xmx%dm" % (profile["xms"], profile["xmx"]))
    task = TaskDefinition("haproxytask")
    task.add_container(container)

    for ec2 in ec2s:
        task.depends_on(ec2)
    return task

def provision_cli(args):
    provision(args.environment, args.stackname, args.apps, args.consul)

def provision(environment, stackname, apps, consul):
    global PROFILE
    PROFILE = load_profile()

    config = AwsEnvironment(environment)
    client = boto3.client('cloudformation')
    check_stack_not_exists(client, stackname)

    create_log_group("docker-ministack-%s" % stackname)

    subnet1 = config.private_subnet_1()
    subnet2 = config.private_subnet_2()
    subnet3 = config.private_subnet_3()

    response = client.create_stack(
        StackName=stackname,
        TemplateURL='https://s3.amazonaws.com/%s' % os.path.join(config.cf_bucket(), _S3_CF_PATH, 'template.json'),
        Parameters=[
            PARAM_VPC_ID.config(config.vpc()),
            PARAM_SUBNET_1.config(subnet1),
            PARAM_SUBNET_2.config(subnet2),
            PARAM_SUBNET_3.config(subnet3),
            PARAM_KEY_NAME.config(config.ec2_key()),
            PARAM_SECURITY_GROUP.config(config.tomcat_sg()),
            PARAM_INSTANCE_TYPE.config('m4.xlarge'),
            PARAM_ENVIRONMENT.config(environment),
            PARAM_CAPACITY.config("0"),
            PARAM_MAX_CAPACITY.config("0"),
            PARAM_TARGET_GROUP.config(DUMMY_TGRP),
            PARAM_ECS_INSTANCE_PROFILE_NAME.config(config.ecs_instance_profile_name()),
            PARAM_ECS_INSTANCE_PROFILE_ARN.config(config.ecs_instance_profile_arn()),
            PARAM_SWAGGER_APPS.config(apps)
        ],
        TimeoutInMinutes=60,
        OnFailure='ROLLBACK',
        Capabilities=[
            'CAPABILITY_IAM',
        ],
        Tags=[
            {
                'Key': 'product',
                'Value': 'lpi'
            }
        ]
    )
    print 'Got StackId: %s' % response['StackId']
    wait_for_stack_creation(client, stackname)

    if consul is not None:
        url = get_proxy_url(stackname)
        write_to_stack(consul, environment, stackname, "HAProxyUrl", url)

def get_proxy_url(stackname):
    stack = boto3.resource('cloudformation').Stack(stackname)
    for output in stack.outputs:
        key = output['OutputKey']
        value = output['OutputValue']
        if key == "HAProxyUrl":
            print "Url for HAProxy is %s" % value
            return value

def teardown_cli(args):
    teardown(args.stackname)

def teardown(stackname):
    client = boto3.client('cloudformation')
    teardown_stack(client, stackname)

def load_profile():
    json_file = os.path.join(TEMPLATE_DIR, 'ministack', 'profile.json')
    with open(json_file) as f:
        return json.loads(f.read())

def parse_args():
    parser = argparse.ArgumentParser(description='ECS cluster CloudFormation cli')
    commands = parser.add_subparsers(help="commands")

    parser1 = commands.add_parser("template")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qacluster','prodcluster'], help='environment')
    parser1.add_argument('-n', dest='instances', type=int, default="1", help='number of instances.')
    parser1.add_argument('-u', dest='upload', action='store_true', help='upload to S3')
    parser1.set_defaults(func=template_cli)

    parser1 = commands.add_parser("provision")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qacluster','prodcluster'], help='environment')
    parser1.add_argument('-s', dest='stackname', type=str, required=True, help='the LE_STACK to be created')
    parser1.add_argument('-a', dest='apps', type=str, help='comma separated list of swagger apps.')
    parser1.add_argument('-c', dest='consul', type=str, help='consul server address')
    parser1.set_defaults(func=provision_cli)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()