"""
ECS stack for tomcat
"""

import argparse
import boto3
import os

from .module.ec2 import ec2_defn
from .module.ecs import ContainerDefinition, TaskDefinition
from .module.parameter import Parameter, EnvVarParameter, ArnParameter
from .module.stack import ECSStack, teardown_stack
from ..conf import AwsEnvironment
from ..ec2.ec2 import register_ec2_to_targetgroup

PARAM_DOCKER_IMAGE=Parameter("DockerImage", "Docker image to be deployed")
PARAM_DOCKER_IMAGE_TAG=Parameter("DockerImageTag", "Docker image tag to be deployed", default="latest")
PARAM_MEM=Parameter("Memory", "Allocated memory for the container")
PARAM_INSTALL_MODE=Parameter("InstallMode", "INTERNAL or EXTERNAL")
PARAM_LE_STACK=Parameter("LEStack", "The name of the parent LE_STACK")
PARAM_ECS_SCALE_ROLE_ARN = ArnParameter("EcsAutoscaleRoleArn", "ECS autoscale role Arn")

_S3_CF_PATH='cloudformation/'

TYPE_DEF=ec2_defn()

def main():
    args = parse_args()
    args.func(args)

def template_cli(args):
    template(args.environment, args.stackname, args.mode, args.profile, args.instances, args.upload)

def template(environment, stackname, mode, profile, instances, upload=False):

    port = 3000 if mode == "EXTERNAL" else 3002

    stack = create_template(profile, instances, port, mode, environment)
    if upload:
        stack.validate()
        stack.upload(environment, s3_path(stackname))
    else:
        print stack.json()
        stack.validate()

def create_template(profile, instances, port, mode, env):
    stack = ECSStack("AWS CloudFormation template for Node.js express server on ECS cluster.", env, use_asgroup=True, instances=instances)
    stack.add_params([PARAM_DOCKER_IMAGE, PARAM_DOCKER_IMAGE_TAG, PARAM_MEM, PARAM_INSTALL_MODE, PARAM_LE_STACK, PARAM_ECS_SCALE_ROLE_ARN])
    profile_vars = get_profile_vars(profile)
    stack.add_params(profile_vars.values())
    task = express_task(env, profile_vars, port, mode)
    stack.add_resource(task)
    stack.add_service("express", task, asrolearn=PARAM_ECS_SCALE_ROLE_ARN)
    return stack

def express_task(environment, profile_vars, port, mode):
    config = AwsEnvironment(environment)
    container = ContainerDefinition("express", { "Fn::Join" : [ "", [
        config.ecr_registry(), "/latticeengines/express:",  PARAM_DOCKER_IMAGE_TAG.ref()]]}) \
        .mem_mb(PARAM_MEM.ref()) \
        .hostname({ "Fn::Join" : ["-", [{ "Ref" : "AWS::StackName" }, "express"]]},) \
        .publish_port(port, 443) \
        .set_logging({
        "LogDriver": "awslogs",
        "Options": {
            "awslogs-group": { "Fn::Join": [ "-", ["lpi", PARAM_LE_STACK.ref()]] },
            "awslogs-region": { "Ref": "AWS::Region" },
            "awslogs-stream-prefix": "lpi" if mode == "EXTERNAL" else "admin-console"
        }}) \
        .set_env("INSTALL_MODE", PARAM_INSTALL_MODE.ref())

    for k, p in profile_vars.items():
        container = container.set_env(k, p.ref())

    task = TaskDefinition("expresstask")
    task.add_container(container)
    return task

def provision_cli(args):
    provision(args.environment, args.stackname, args.tgrp, args.profile, args.instancetype, args.mode, args.instances, tag=args.tag, public=args.public, le_stack=args.lestack)


def provision(environment, stackname, tgrp, profile, instance_type, mode, instances, tag="latest", public=False, le_stack=None):
    profile_vars = get_profile_vars(profile)
    extra_params = parse_profile(profile, profile_vars)

    max_mem = TYPE_DEF[instance_type]['mem_gb'] * 1024 - 512
    print "set %s to %s" % (PARAM_MEM.name(), max_mem)
    extra_params.append(PARAM_MEM.config(str(max_mem)))

    if le_stack is None:
        le_stack = stackname

    config = AwsEnvironment(environment)    
    extra_params.append(PARAM_DOCKER_IMAGE.config("express"))
    extra_params.append(PARAM_DOCKER_IMAGE_TAG.config(tag))
    extra_params.append(PARAM_INSTALL_MODE.config(mode))
    extra_params.append(PARAM_LE_STACK.config(le_stack))
    extra_params.append(PARAM_ECS_SCALE_ROLE_ARN.config(config.ecs_autoscale_role_arn()))
    tgrp_arn = find_tgrp_arn(tgrp)

    ECSStack.provision(environment, s3_path(stackname), stackname, config.nodejs_sg(), tgrp_arn, init_cap=instances, max_cap=instances, public=public, instance_type=instance_type, additional_params=extra_params, le_stack=le_stack)

    register_ec2_to_targetgroup(stackname, tgrp)


def teardown_cli(args):
    teardown(args.stackname)

def teardown(stackname):
    client = boto3.client('cloudformation')
    teardown_stack(client, stackname)

def get_profile_vars(profile):
    params = {}
    if profile is not None:
        with open(profile, 'r') as file:
            for line in file:
                line = line.strip().replace('\n', '')
                if len(line) > 0 and ('#' != line[0]):
                    key = line.split('=')[0]
                    params[key] = EnvVarParameter(key)
    return params

def parse_profile(profile, profile_vars):
    params = []
    if profile is not None:
        with open(profile, 'r') as file:
            for line in file:
                line = line.strip().replace('\n', '')
                if len(line) > 0 and ('#' != line[0]):
                    key = line.split('=')[0]
                    value = line[len(key) + 1:]
                    if key in profile_vars:
                        print "set %s to %s" % (key, value)
                        params.append(profile_vars[key].config(value))

    return params

def find_tgrp_arn(name):
    client = boto3.client('elbv2')
    response = client.describe_target_groups()
    for tgrp in response['TargetGroups']:
        if tgrp['TargetGroupName'] == name:
            tgrp_arn = tgrp['TargetGroupArn']
            print "Found target group " + tgrp_arn
            return tgrp_arn
    raise Exception("Cannot find target group named "+ name)


def s3_path(stackname):
    return os.path.join(_S3_CF_PATH, stackname)

def parse_args():
    parser = argparse.ArgumentParser(description='ECS cluster CloudFormation cli')
    commands = parser.add_subparsers(help="commands")

    parser1 = commands.add_parser("template")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qacluster','prodcluster'], help='environment')
    parser1.add_argument('-u', dest='upload', action='store_true', help='upload to S3')
    parser1.add_argument('-s', dest='stackname', type=str, required=True, help='stack name')
    parser1.add_argument('-n', dest='instances', type=int, default="1", help='number of instances.')
    parser1.add_argument('-m', dest='mode', type=str, default='EXTERNAL', help='INTERNAL or EXTERNAL. INTERNAL means admin console. EXTERNAL means lpi')
    parser1.add_argument('-p', dest='profile', type=str, help='stack profile file')
    parser1.set_defaults(func=template_cli)

    parser1 = commands.add_parser("provision")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qacluster','prodcluster'], help='environment')
    parser1.add_argument('-t', dest='tag', type=str, default='latest', help='docker image tag')
    parser1.add_argument('-s', dest='stackname', type=str, required=True, help='stack name')
    parser1.add_argument('-g', dest='tgrp', type=str, required=True, help='name of the target group for load balancer')
    parser1.add_argument('-p', dest='profile', type=str, help='stack profile file')
    parser1.add_argument('-i', dest='instancetype', type=str, default='t2.medium', help='EC2 instance type')
    parser1.add_argument('-m', dest='mode', type=str, default='EXTERNAL', help='INTERNAL or EXTERNAL. INTERNAL means admin console. EXTERNAL means lpi')
    parser1.add_argument('-n', dest='instances', type=int, default="1", help='number of instances.')
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
