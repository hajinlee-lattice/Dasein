"""
ECS stack for zookeeper
"""

import argparse
import boto3
import os

from .module.ecs import ContainerDefinition, TaskDefinition
from .module.elb import ElasticLoadBalancer
from .module.parameter import *
from .module.stack import ECSStack, teardown_stack, check_stack_not_exists, wait_for_stack_creation
from ..conf import AwsEnvironment

_S3_CF_PATH='cloudformation/zk'
PARAM_ELB_DNSNAME=Parameter("ElbDnsName", "DNS name for elb")

def main():
    args = parse_args()
    args.func(args)

def template_cli(args):
    template(args.environment, args.upload)

def template(environment, upload=False):
    stack = create_template()
    if upload:
        stack.validate()
        stack.upload(environment, _S3_CF_PATH)
    else:
        print stack.json()
        stack.validate()

def create_template():
    elb5000 = ElasticLoadBalancer("lb5000").listen("5000").internal()

    stack = ECSStack("AWS CloudFormation template for ECS cluster.", extra_elbs=[ elb5000 ], use_external_elb=False)
    task = discover_task()
    stack.add_resource(task)
    stack.add_service("discover", task, capacity=1)
    stack.add_resource(elb5000)

    return stack

def discover_task():
    container = ContainerDefinition("discover", { "Fn::Join" : [ "", [
        { "Fn::FindInMap" : [ "Environment2Props", {"Ref" : "Environment"}, "EcrRegistry" ] },
        "/latticeengines/discover" ] ]}) \
        .mem_mb("1024").publish_port(5000, 5000) \
        .set_logging({
        "LogDriver": "awslogs",
        "Options": {
            "awslogs-group": "docker-discover",
            "awslogs-region": { "Ref": "AWS::Region" }
        }})

    task = TaskDefinition("discovertask")
    task.add_container(container)
    return task

def provision_cli(args):
    provision(args.environment, args.stackname, args.elb, quorum_size=args.size)

def provision(environment, stackname, zk_elb, quorum_size=3):
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
            PARAM_INSTANCE_TYPE.config('t2.medium'),
            PARAM_ENVIRONMENT.config(environment),
            PARAM_ECS_INSTANCE_PROFILE_ARN.config(config.ecs_instance_profile_arn()),
            PARAM_ELB_NAME.config(zk_elb),
            PARAM_CAPACITY.config(str(quorum_size)),
            PARAM_MAX_CAPACITY.config(str(quorum_size))
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


def teardown_cli(args):
    teardown(args.stackname)

def teardown(stackname):
    client = boto3.client('cloudformation')
    teardown_stack(client, stackname)

def parse_args():
    parser = argparse.ArgumentParser(description='ECS cluster CloudFormation cli')
    commands = parser.add_subparsers(help="commands")

    parser1 = commands.add_parser("template")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qa','prod'], help='environment')
    parser1.add_argument('-u', dest='upload', action='store_true', help='upload to S3')
    parser1.set_defaults(func=template_cli)

    parser1 = commands.add_parser("provision")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qa','prod'], help='environment')
    parser1.add_argument('-s', dest='stackname', type=str, default='ecscluster', help='stack name')
    parser1.add_argument('-b', dest='elb', type=str, required=True, help='name of the elastic load balancer')
    parser1.add_argument('-n', dest='size', type=int, default='3', help='quorum size')
    parser1.set_defaults(func=provision_cli)

    parser1 = commands.add_parser("teardown")
    parser1.add_argument('-s', dest='stackname', type=str, default='ecscluster', help='stack name')
    parser1.set_defaults(func=teardown_cli)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
