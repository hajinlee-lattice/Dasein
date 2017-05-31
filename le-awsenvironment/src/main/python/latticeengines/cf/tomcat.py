"""
ECS stack for tomcat
"""

import argparse
import boto3
import os

from .module.ec2 import ec2_defn
from .module.ecs import ContainerDefinition, TaskDefinition, Volume
from .module.parameter import Parameter, EnvVarParameter, ArnParameter
from .module.stack import ECSStack, teardown_stack
from ..conf import AwsEnvironment
from ..ec2.ec2 import register_ec2_to_targetgroup
from ..elb.targetgroup import find_tgrp_arn

PARAM_DOCKER_IMAGE=Parameter("DockerImage", "Docker image to be deployed")
PARAM_DOCKER_IMAGE_TAG=Parameter("DockerImageTag", "Docker image tag to be deployed", default="latest")
PARAM_MEM=Parameter("Memory", "Allocated memory for the container")
PARAM_ENV_CATALINA_OPTS=EnvVarParameter("CATALINA_OPTS")
PARAM_EFS = Parameter("Efs", "EFS Id")
PARAM_SNS_TOPIC_ARN = ArnParameter("SNSTopicArn", "SNS Topic Arn")
PARAM_ECS_SCALE_ROLE_ARN = ArnParameter("EcsAutoscaleRoleArn", "ECS autoscale role Arn")
PARAM_SECOND_TGRP_ARN = ArnParameter("SecondTgrpArn", "Secondary target group arn")
PARAM_SPLUNK_URL=Parameter("SplunkUrl", "Url of splunk collector")
PARAM_SPLUNK_TOKEN=Parameter("SplunkToken", "Splunk token")

_S3_CF_PATH='cloudformation/'

TYPE_DEF=ec2_defn()

def main():
    args = parse_args()
    args.func(args)

def template_cli(args):
    template(args.environment, args.stackname, args.profile, fixed_instances=args.fixed, num_instances=args.numinstances, second_tgrp=args.secondtgrp, upload=args.upload)

def template(environment, stackname, profile, fixed_instances=False, num_instances=1, second_tgrp=False, upload=False):
    stack = create_template(environment, profile, fixed_instances, num_instances, second_tgrp=second_tgrp)
    if upload:
        stack.validate()
        stack.upload(environment, s3_path(stackname))
    else:
        print stack.json()
        stack.validate()

def create_template(environment, profile, fixed_instances=False, num_instances=1, second_tgrp=False):
    stack = ECSStack("AWS CloudFormation template for Tomcat server on ECS cluster.", environment, use_asgroup=(not fixed_instances), instances=num_instances, efs=PARAM_EFS, sns_topic=PARAM_SNS_TOPIC_ARN)
    stack.add_params([
        PARAM_DOCKER_IMAGE,
        PARAM_DOCKER_IMAGE_TAG,
        PARAM_MEM,
        PARAM_ENV_CATALINA_OPTS,
        PARAM_EFS,
        PARAM_SNS_TOPIC_ARN,
        PARAM_ECS_SCALE_ROLE_ARN,
        PARAM_SPLUNK_URL,
        PARAM_SPLUNK_TOKEN
    ])
    if second_tgrp:
        stack.add_param(PARAM_SECOND_TGRP_ARN)
        if not fixed_instances:
            stack.attach_tgrp(PARAM_SECOND_TGRP_ARN)
    profile_vars = get_profile_vars(profile)
    stack.add_params(profile_vars.values())
    task = tomcat_task(profile_vars, env=environment)
    stack.add_resource(task)
    service, tgt = stack.add_service("tomcat", task, asrolearn=PARAM_ECS_SCALE_ROLE_ARN)
    if not fixed_instances:
        stack.percent_autoscale(tgt, "ScaleUp", 100, 300, lb=0)
        stack.exact_autoscale(tgt, "ScaleBack", num_instances, 600, ub=0)
    return stack

def tomcat_task(profile_vars, env="qa"):
    container = ContainerDefinition("tomcat", { "Fn::Join" : [ "", [
        { "Fn::FindInMap" : [ "Environment2Props", {"Ref" : "Environment"}, "EcrRegistry" ] },
        "/latticeengines/", PARAM_DOCKER_IMAGE.ref(), ":",  PARAM_DOCKER_IMAGE_TAG.ref()]]}) \
        .mem_mb(PARAM_MEM.ref()) \
        .hostname({ "Fn::Join" : ["-", [{ "Ref" : "AWS::StackName" }, PARAM_DOCKER_IMAGE.ref()]]}) \
        .publish_port(8080, 80) \
        .publish_port(8443, 443) \
        .publish_port(1099, 1099) \
        .add_docker_label("stack", profile_vars["LE_STACK"].ref()) \
        .add_docker_label("app", PARAM_DOCKER_IMAGE.ref()) \
        .privileged()

    container = container.set_logging({
        "LogDriver": "splunk",
        "Options": {
            "splunk-url": PARAM_SPLUNK_URL.ref(),
            "splunk-token": PARAM_SPLUNK_TOKEN.ref(),
            "splunk-index": "main",
            "splunk-sourcetype": "log4j",
            "labels": "stack,app"
        }})


    for k, p in profile_vars.items():
        container = container.set_env(k, p.ref())
    container.set_env("HADOOP_CONF_DIR", "/etc/hadoop/conf")

    ledp = Volume("ledp", "/etc/ledp")
    efsip = Volume("efsip", "/etc/efsip.txt")
    scoringcache = Volume("scoringcache", "/var/cache/scoringapi")
    internal_addr = Volume("intAddr", "/etc/internaladdr.txt")
    hadoop_conf = Volume("hadoopConf", "/etc/hadoop/conf")

    container = container.mount("/etc/ledp", ledp) \
        .mount("/etc/efsip.txt", efsip) \
        .mount("/var/cache/scoringapi", scoringcache) \
        .mount("/etc/internaladdr.txt", internal_addr) \
        .mount("/etc/hadoop/conf", hadoop_conf)

    task = TaskDefinition("tomcattask")
    task.add_container(container)
    task.add_volume(ledp)
    task.add_volume(efsip)
    task.add_volume(scoringcache)
    task.add_volume(internal_addr)
    task.add_volume(hadoop_conf)
    return task

def provision_cli(args):
    provision(args.environment, args.app, args.stackname, args.tgrp, args.profile, args.instancetype, tag=args.tag, init_cap=args.ic, max_cap=args.mc, public=args.public, le_stack=args.lestack, second_tgrp=args.secondtgrp)


def provision(environment, app, stackname, tgrp, profile, instance_type, tag="latest", init_cap=2, max_cap=8, public=False, le_stack=None, second_tgrp=None):
    profile_vars = get_profile_vars(profile)
    extra_params = parse_profile(profile, profile_vars)

    max_mem = int(TYPE_DEF[instance_type]['mem_gb'] * 1024.0 - 512)
    print "set %s to %s" % (PARAM_MEM.name(), max_mem)
    extra_params.append(PARAM_MEM.config(str(max_mem)))

    opts=update_xmx_by_type("", instance_type)
    print "set %s to %s" % ('CATALINA_OPTS', opts)
    extra_params.append(PARAM_ENV_CATALINA_OPTS.config(opts))

    extra_params.append(PARAM_DOCKER_IMAGE.config(app))
    extra_params.append(PARAM_DOCKER_IMAGE_TAG.config(tag))

    tgrp_arn = find_tgrp_arn(tgrp)
    config = AwsEnvironment(environment)
    sg = config.tomcat_sg()

    extra_params.append(PARAM_EFS.config(config.lpi_efs_id()))
    extra_params.append(PARAM_SNS_TOPIC_ARN.config(config.scaling_sns_topic_arn()))
    extra_params.append(PARAM_ECS_SCALE_ROLE_ARN.config(config.ecs_autoscale_role_arn()))
    extra_params.append(PARAM_SPLUNK_URL.config(config.splunk_url()))
    extra_params.append(PARAM_SPLUNK_TOKEN.config(config.splunk_token()))

    if (second_tgrp is not None) and (second_tgrp != ""):
        second_tgrp_arn = find_tgrp_arn(second_tgrp)
        extra_params.append(PARAM_SECOND_TGRP_ARN.config(second_tgrp_arn))

    ECSStack.provision(environment, s3_path(stackname), stackname, sg, tgrp_arn, init_cap=init_cap, max_cap=max_cap, public=public, instance_type=instance_type, additional_params=extra_params, le_stack=le_stack)

    register_ec2_to_targetgroup(stackname, tgrp)
    if (second_tgrp is not None) and (second_tgrp != ""):
        register_ec2_to_targetgroup(stackname, second_tgrp)

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
    opt2 = "-XX:ReservedCodeCacheSize=256m"

    opts = ''
    tokens = catalina_opts.split(' ')
    replaced = False
    for token in tokens:
        if '-Xmx' in token:
            opts = catalina_opts.replace(token, opt)
            replaced = True

    if not replaced:
       opts = (opt + " " + opt2 + " " + catalina_opts).strip()

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
    parser1.add_argument('--fixed-instances', dest='fixed', action='store_true', help='use fixed number of instances, instead of auto scaling group')
    parser1.add_argument('--second-tgrp', dest='secondtgrp', action='store_true', help='use secondary tgrp')
    parser1.add_argument('-n', dest='numinstances', type=int, default="1", help='number of instances. only honored when --fixed-instances option is used.')
    parser1.add_argument('-p', dest='profile', type=str, help='stack profile file')
    parser1.set_defaults(func=template_cli)

    parser1 = commands.add_parser("provision")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qacluster','prodcluster'], help='environment')
    parser1.add_argument('-a', dest='app', type=str, required=True, help='application name (same as docker image name)')
    parser1.add_argument('-t', dest='tag', type=str, default='latest', help='docker image tag')
    parser1.add_argument('-s', dest='stackname', type=str, required=True, help='stack name')
    parser1.add_argument('-g', dest='tgrp', type=str, required=True, help='name of the target group for load balancer')
    parser1.add_argument('-p', dest='profile', type=str, help='stack profile file')
    parser1.add_argument('-i', dest='instancetype', type=str, default='t2.medium', help='EC2 instance type')
    parser1.add_argument('--catalina-opts', dest='copts', type=str, default='', help='CATALINA_OPTS')
    parser1.add_argument('--public', dest='public', action='store_true', help='use public subnets')
    parser1.add_argument('--initial-capacity', dest='ic', type=int, default='2', help='initial capacity. only honored when using auto scaling group.')
    parser1.add_argument('--max-capacity', dest='mc', type=int, default='8', help='maximum capacity. only honored when using auto scaling group.')
    parser1.add_argument('--le-stack', dest='lestack', type=str, help='the parent LE_STACK')
    parser1.add_argument('--second-tgrp', dest='secondtgrp', type=str, help='name of the secondary tgrp')
    parser1.set_defaults(func=provision_cli)

    parser1 = commands.add_parser("teardown")
    parser1.add_argument('-s', dest='stackname', type=str, default='ecscluster', help='stack name')
    parser1.set_defaults(func=teardown_cli)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
