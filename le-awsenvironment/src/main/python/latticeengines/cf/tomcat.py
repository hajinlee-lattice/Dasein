"""
ECS stack for tomcat
"""

import argparse
import boto3
import os

from .module.ec2 import ec2_defn
from .module.ecs import ContainerDefinition, TaskDefinition
from .module.parameter import Parameter, EnvVarParameter
from .module.stack import ECSStack, teardown_stack

PARAM_DOCKER_IMAGE=Parameter("DockerImage", "Docker image to be deployed")
PARAM_DOCKER_IMAGE_TAG=Parameter("DockerImageTag", "Docker image tag to be deployed", default="latest")
PARAM_MEM=Parameter("Memory", "Allocated memory for the container")
PARAM_ENV_CATALINA_OPTS=EnvVarParameter("CATALINA_OPTS")

_S3_CF_PATH='cloudformation/'

TYPE_DEF=ec2_defn()

def main():
    args = parse_args()
    args.func(args)

def template_cli(args):
    template(args.environment, args.stackname, args.profile, args.upload)

def template(environment, stackname, profile, upload=False):
    stack = create_template(profile)
    if upload:
        stack.validate()
        stack.upload(environment, s3_path(stackname))
    else:
        print stack.json()
        stack.validate()

def create_template(profile):
    stack = ECSStack("AWS CloudFormation template for ECS cluster.")
    stack.add_params([PARAM_DOCKER_IMAGE, PARAM_DOCKER_IMAGE_TAG, PARAM_MEM, PARAM_ENV_CATALINA_OPTS])
    profile_vars = get_profile_vars(profile)
    stack.add_params(profile_vars.values())
    task = tomcat_task(profile_vars)
    stack.add_resource(task)
    stack.add_service("tomcat", task)
    return stack

def tomcat_task(profile_vars):
    container = ContainerDefinition("httpd", { "Fn::Join" : [ "", [
        { "Fn::FindInMap" : [ "Environment2Props", {"Ref" : "Environment"}, "EcrRegistry" ] },
        "/latticeengines/", PARAM_DOCKER_IMAGE.ref(), ":",  PARAM_DOCKER_IMAGE_TAG.ref()]]}) \
        .mem_mb(PARAM_MEM.ref()) \
        .publish_port(8080, 80) \
        .publish_port(8443, 443) \
        .publish_port(1099, 1099) \
        .set_logging({
        "LogDriver": "awslogs",
        "Options": {
            "awslogs-group": { "Fn::Join" : ["", ["docker-", PARAM_DOCKER_IMAGE.ref()]]},
            "awslogs-region": { "Ref": "AWS::Region" }
        }})

    for k, p in profile_vars.items():
        container = container.set_env(k, p.ref())

    task = TaskDefinition("tomcattask")
    task.add_container(container)
    return task

def provision_cli(args):
    provision(args.environment, args.app, args.stackname, args.elb, args.profile, args.instancetype, tag=args.tag, init_cap=args.ic, max_cap=args.mc, public=args.public)


def provision(environment, app, stackname, elb, profile, instance_type, tag="latest", init_cap=2, max_cap=8, public=False):
    profile_vars = get_profile_vars(profile)
    extra_params = parse_profile(profile, profile_vars)

    max_mem = TYPE_DEF[instance_type]['mem_gb'] * 1024 - 256
    print "set %s to %s" % (PARAM_MEM.name(), max_mem)
    extra_params.append(PARAM_MEM.config(str(max_mem)))

    opts=update_xmx_by_type("", instance_type)
    print "set %s to %s" % ('CATALINA_OPTS', opts)
    extra_params.append(PARAM_ENV_CATALINA_OPTS.config(opts))

    extra_params.append(PARAM_DOCKER_IMAGE.config(app))
    extra_params.append(PARAM_DOCKER_IMAGE_TAG.config(tag))

    ECSStack.provision(environment, s3_path(stackname), stackname, elb, init_cap=init_cap, max_cap=max_cap, public=public, instance_type=instance_type, additional_params=extra_params)

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

def update_xmx_by_type(catalina_opts, instance_type):
    max_mem = TYPE_DEF[instance_type]['mem_gb'] * 1024 - 300
    opt = '-Xmx%dm' % max_mem
    opts = ''
    tokens = catalina_opts.split(' ')
    replaced = False
    for token in tokens:
        if '-Xmx' in token:
            opts = catalina_opts.replace(token, opt)
            replaced = True

    if not replaced:
       opts = (opt + " " + catalina_opts).strip()

    return opts

def s3_path(stackname):
    return os.path.join(_S3_CF_PATH, stackname)

def parse_args():
    parser = argparse.ArgumentParser(description='ECS cluster CloudFormation cli')
    commands = parser.add_subparsers(help="commands")

    parser1 = commands.add_parser("template")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qacluster','prodcluster'], help='environment')
    parser1.add_argument('-u', dest='upload', action='store_true', help='upload to S3')
    parser1.add_argument('-s', dest='stackname', type=str, required=True, help='stack name')
    parser1.add_argument('-p', dest='profile', type=str, help='stack profile file')
    parser1.set_defaults(func=template_cli)

    parser1 = commands.add_parser("provision")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qacluster','prodcluster'], help='environment')
    parser1.add_argument('-a', dest='app', type=str, required=True, help='application name (same as docker image name)')
    parser1.add_argument('-t', dest='tag', type=str, default='latest', help='docker image tag')
    parser1.add_argument('-s', dest='stackname', type=str, required=True, help='stack name')
    parser1.add_argument('-b', dest='elb', type=str, required=True, help='name of the elastic load balancer')
    parser1.add_argument('-p', dest='profile', type=str, help='stack profile file')
    parser1.add_argument('-i', dest='instancetype', type=str, default='t2.medium', help='EC2 instance type')
    parser1.add_argument('--catalina-opts', dest='copts', type=str, default='', help='CATALINA_OPTS')
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
