"""
Example ecs stack for a httpd service
"""

import argparse
import boto3

from .module.ecs import ContainerDefinition, TaskDefinition
from .module.stack import ECSStack, teardown_stack

_S3_CF_PATH='cloudformation/ecscluster'

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
    stack = ECSStack("AWS CloudFormation template for ECS cluster.")
    task = httpd_task()
    stack.add_resource(task)
    stack.add_service("webapp", task)
    return stack

def httpd_task():
    container = ContainerDefinition("httpd", { "Fn::Join" : [ "", [
        { "Fn::FindInMap" : [ "Environment2Props", {"Ref" : "Environment"}, "EcrRegistry" ] },
        "/latticeengines/httpd"
    ] ]}) \
        .mem_mb("1024").publish_port(80, 80).publish_port(443, 443) \
        .set_logging({
        "LogDriver": "awslogs",
        "Options": {
            "awslogs-group": "docker-httpd",
            "awslogs-region": { "Ref": "AWS::Region" }
        }
    })

    task = TaskDefinition("webapptask")
    task.add_container(container)
    return task

def provision_cli(args):
    provision(args.environment, args.stackname, args.elb, init_cap=args.ic, max_cap=args.mc, public=args.public)

def provision(environment, stackname, elb, init_cap=2, max_cap=8, public=False):
    ECSStack.provision(environment, _S3_CF_PATH, stackname, elb, init_cap=init_cap, max_cap=max_cap, public=public)

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
    parser1.add_argument('--public', dest='public', action='store_true', help='use public subnets')
    parser1.set_defaults(func=template_cli)

    parser1 = commands.add_parser("provision")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qa','prod'], help='environment')
    parser1.add_argument('-s', dest='stackname', type=str, default='ecscluster', help='stack name')
    parser1.add_argument('-b', dest='elb', type=str, required=True, help='name of the elastic load balancer')
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
