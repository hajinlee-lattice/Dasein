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

_S3_CF_PATH='cloudformation/observer'

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
    config = AwsEnvironment(environment)
    ips = config.zk_observer_ips()

    stack = ECSStack("AWS CloudFormation template for ECS cluster.", use_asgroup=False, instances=2, ips=ips)
    task = observer_task()
    stack.add_resource(task)
    stack.add_service("observer", task, capacity=2)

    return stack

def observer_task():
    container = ContainerDefinition("observer", { "Fn::Join" : [ "", [
        { "Fn::FindInMap" : [ "Environment2Props", PARAM_ENVIRONMENT.ref(), "EcrRegistry" ] },
        "/latticeengines/observer" ] ]}) \
        .mem_mb("1700").publish_port(2181, 2181) \
        .set_logging({
        "LogDriver": "awslogs",
        "Options": {
            "awslogs-group": "docker-observer",
            "awslogs-region": { "Ref": "AWS::Region" }
        }}) \
        .set_env("LE_ENVIRONMENT", PARAM_ENVIRONMENT.ref()) \
        .set_env("JVMFLAGS", "-Xms1g -Xmx1700m") \
        .set_env("ZOO_LOG_DIR", "/var/log/zookeeper")

    task = TaskDefinition("observertask")
    task.add_container(container)
    return task

def provision_cli(args):
    provision(args.environment, args.stackname)

def provision(environment, stackname):
    config = AwsEnvironment(environment)
    client = boto3.client('cloudformation')
    check_stack_not_exists(client, stackname)

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
            PARAM_SECURITY_GROUP.config(config.zk_sg()),
            PARAM_INSTANCE_TYPE.config('t2.small'),
            PARAM_ENVIRONMENT.config(environment),
            PARAM_CAPACITY.config("2"),
            PARAM_MAX_CAPACITY.config("2"),
            PARAM_TARGET_GROUP.config("arn:aws:iam::028036828464:target-group/dummy"),
            PARAM_ECS_INSTANCE_PROFILE.config(config.ecs_instance_profile())
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
    parser1.set_defaults(func=provision_cli)

    parser1 = commands.add_parser("teardown")
    parser1.add_argument('-s', dest='stackname', type=str, default='ecscluster', help='stack name')
    parser1.set_defaults(func=teardown_cli)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
