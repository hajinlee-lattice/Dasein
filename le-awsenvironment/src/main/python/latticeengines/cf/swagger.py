"""
ECS stack for zookeeper
"""

import argparse
import boto3
import os

from .module.ecs import ContainerDefinition, TaskDefinition
from .module.parameter import *
from .module.stack import ECSStack, teardown_stack, check_stack_not_exists, wait_for_stack_creation
from ..conf import AwsEnvironment
from ..ec2.ec2 import register_ec2_to_targetgroup
from ..elb.targetgroup import find_tgrp_arn

_S3_CF_PATH='cloudformation/swagger'

PARAM_SWAGGER_APPS=Parameter("SwaggerApps", "List of apps for swagger.")
PARAM_LE_STACK=Parameter("LEStack", "The name of the parent LE_STACK")

def main():
    args = parse_args()
    args.func(args)

def template_cli(args):
    template(args.environment, args.upload)

def template(environment, upload=False):
    stack = create_template(environment)
    if upload:
        stack.validate()
        stack.upload(environment, _S3_CF_PATH)
    else:
        print stack.json()
        stack.validate()

def create_template(environment):
    stack = ECSStack("AWS CloudFormation template for swagger ECS cluster.", environment, use_asgroup=False, instances=1)
    stack.add_params([PARAM_SWAGGER_APPS, PARAM_LE_STACK])
    task = swagger_task()
    stack.add_resource(task)
    stack.add_service("swagger", task, capacity=1)

    return stack

def swagger_task():
    container = ContainerDefinition("httpd", { "Fn::Join" : [ "", [
        { "Fn::FindInMap" : [ "Environment2Props", PARAM_ENVIRONMENT.ref(), "EcrRegistry" ] },
        "/latticeengines/swagger" ] ]}) \
        .mem_mb("1700") \
        .publish_port(80, 80) \
        .publish_port(443, 443) \
        .set_logging({
        "LogDriver": "awslogs",
        "Options": {
            "awslogs-group": { "Fn::Join": ["-", ["lpi", PARAM_LE_STACK.ref()]] },
            "awslogs-region": { "Ref": "AWS::Region" },
            "awslogs-stream-prefix": "swagger"
        }}) \
        .set_env("SWAGGER_APPS", PARAM_SWAGGER_APPS.ref()) \
        .set_env("JVMFLAGS", "-Xms1g -Xmx1700m")
    task = TaskDefinition("swaggertask")
    task.add_container(container)
    return task

def provision_cli(args):
    provision(args.environment, args.stackname, args.apps, args.tgrp, public=args.public, le_stack=args.lestack)

def provision(environment, stackname, apps, tgrp, public=False, le_stack=None):
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

    if le_stack is None:
        le_stack = stackname

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
            PARAM_INSTANCE_TYPE.config('t2.small'),
            PARAM_ENVIRONMENT.config(environment),
            PARAM_CAPACITY.config("1"),
            PARAM_MAX_CAPACITY.config("1"),
            PARAM_TARGET_GROUP.config(find_tgrp_arn(tgrp)),
            PARAM_ECS_INSTANCE_PROFILE_NAME.config(config.ecs_instance_profile_name()),
            PARAM_ECS_INSTANCE_PROFILE_ARN.config(config.ecs_instance_profile_arn()),
            PARAM_ECS_INSTANCE_ROLE_NAME.config(config.ecs_instance_role_name()),
            PARAM_SWAGGER_APPS.config(apps),
            PARAM_LE_STACK.config(le_stack)
        ],
        TimeoutInMinutes=60,
        OnFailure='ROLLBACK',
        Capabilities=[
            'CAPABILITY_IAM',
        ],
        Tags=[
            {
                'Key': 'le-env',
                'Value': environment.replace("cluster", "")
            },
            {
                'Key': 'le-product',
                'Value': 'lpi'
            },
            {
                'Key': 'le-service',
                'Value': 'swagger'
            },
            {
                'Key': 'le-stack',
                'Value': le_stack
            }
        ]
    )
    print 'Got StackId: %s' % response['StackId']
    wait_for_stack_creation(client, stackname)
    register_ec2_to_targetgroup(stackname, tgrp)

def teardown_cli(args):
    teardown(args.stackname)

def teardown(stackname):
    client = boto3.client('cloudformation')
    teardown_stack(client, stackname)

def parse_args():
    parser = argparse.ArgumentParser(description='ECS cluster CloudFormation cli')
    commands = parser.add_subparsers(help="commands")

    parser1 = commands.add_parser("template")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qacluster','prodcluster'], help='environment')
    parser1.add_argument('-u', dest='upload', action='store_true', help='upload to S3')
    parser1.set_defaults(func=template_cli)

    parser1 = commands.add_parser("provision")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qacluster','prodcluster'], help='environment')
    parser1.add_argument('-s', dest='stackname', type=str, default='ecscluster', help='stack name')
    parser1.add_argument('-g', dest='tgrp', type=str, required=True, help='name of the target group for load balancer')
    parser1.add_argument('-a', dest='apps', type=str, help='comma separated list of swagger apps.')
    parser1.add_argument('--public', dest='public', action='store_true', help='use public subnets')
    parser1.add_argument('--le-stack', dest='lestack', type=str, help='the parent LE_STACK')
    parser1.set_defaults(func=provision_cli)

    parser1 = commands.add_parser("teardown")
    parser1.add_argument('-s', dest='stackname', type=str, default='ecscluster', help='stack name')
    parser1.set_defaults(func=teardown_cli)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
