"""
ECS stack for zookeeper
"""

import argparse
import boto3
import json
import os
import threading
import time

from .consul import write_to_stack, read_from_stack
from .module.ecs import ContainerDefinition, TaskDefinition, Volume
from .module.parameter import *
from .module.stack import ECSStack, check_stack_not_exists, wait_for_stack_creation, teardown_stack
from .module.template import TEMPLATE_DIR
from ..conf import AwsEnvironment
from ..cw.logs import clean_internal as clean_log_group
from ..cw.logs import create_internal as create_log_group
from ..ecs.container import Container
from ..ecs.manage import register_task, deregister_task, create_service, delete_service, find_cluster_random_token
from ..ecs.volume import Volume as ECSVolume
from ..elb.targetgroup import DUMMY_TGRP

_S3_CF_PATH='cloudformation/ministack/'

PARAM_DOCKER_IMAGE_TAG=Parameter("DockerImageTag", "Docker image tag to be deployed", default="latest")
PARAM_EFS = Parameter("Efs", "EFS Id")

ALL_APPS="ui,pls,admin,matchapi,saml,scoringapi,oauth2,playmaker,eai,metadata,scoring,modeling,dataflowapi,workflowapi,quartz,modelquality,propdata,dellebi,datacloudapi,lp"
DEFAULT_APPS="pls,admin,matchapi,saml,scoringapi,oauth2,playmaker,eai,metadata,scoring,modeling,dataflowapi,workflowapi"
ALLOCATION = {}
HAPROXY_KEY="HAProxy"

class CreateServiceThread (threading.Thread):
    def __init__(self, environment, stackname, app, instances, ip, profile, tag, logdriver):
        threading.Thread.__init__(self)
        self.threadID = "%s-%s" % (stackname, app)
        self.environment = environment
        self.stackname = stackname
        self.app = app
        self.instances = instances
        self.ip = ip
        self.profile = profile
        self.tag = tag
        self.logdriver = logdriver

    def run(self):
        if self.app == "ui":
            container = ui_container(self.environment, self.stackname, self.app, self.ip, self.profile, self.tag)
        else:
            container = tomcat_container(self.environment, self.stackname, self.app, self.ip, self.profile, self.tag, self.logdriver)
        ledp = ECSVolume("ledp", "/etc/ledp")
        ledpLog = ECSVolume("ledpLog", "/var/log/ledp")
        internal_addr = ECSVolume("intAddr", "/etc/internaladdr.txt")
        token = find_cluster_random_token(self.stackname)
        task = "%s-%s-%s" % (self.stackname, self.app, token)
        register_task(task, [container], [ledp, ledpLog, internal_addr])
        create_service(self.stackname, self.app, task, self.instances)


class DeleteServiceThread (threading.Thread):
    def __init__(self, stackname, app):
        threading.Thread.__init__(self)
        self.threadID = "%s-%s" % (stackname, app)
        self.stackname = stackname
        self.app = app

    def run(self):
        delete_service(self.stackname, self.app)
        task = "%s-%s-" % (self.stackname, self.app)
        deregister_task(task)

def main():
    args = parse_args()
    args.func(args)

def template_cli(args):
    template(args.environment, args.stackname, args.instances, args.apps, upload=args.upload)

def template(environment, stackname, instances, apps, upload=False):
    infra_stack = create_infra_template(environment, stackname, instances, apps)
    if upload:
        infra_stack.validate()
        infra_stack.upload(environment, infra_stack_s3(stackname))
    else:
        print infra_stack.json()
        infra_stack.validate()

def create_infra_template(environment, stackname, instances, apps):
    stack = ECSStack("AWS CloudFormation template for mini-stack infrastructure.", environment, use_asgroup=False, instances=instances, efs=PARAM_EFS)
    stack.add_params([PARAM_EFS, PARAM_DOCKER_IMAGE_TAG])

    config = AwsEnvironment(environment)
    task1 = swagger_task(stackname, apps, config.ecr_registry())
    stack.add_resource(task1)
    stack.add_service("swagger", task1, capacity=1)

    task2 = haproxy_task(stackname, stack.get_ec2s(), config.ecr_registry())
    stack.add_resource(task2)
    haproxy, _ = stack.create_service("haproxy", task2, capacity=instances)
    stack.add_resource(haproxy)

    return stack

def swagger_task(stackname, apps, ecr):
    ledp = Volume("ledp", "/etc/ledp")
    container = ContainerDefinition("httpd", ecr + "/latticeengines/swagger") \
        .mem_mb("256") \
        .publish_port(80, 8080) \
        .set_logging({
        "LogDriver": "awslogs",
        "Options": {
            "awslogs-group": "ministack-%s" % stackname,
            "awslogs-region": { "Ref": "AWS::Region" },
            "awslogs-stream-prefix": "swagger"
        }}) \
        .set_env("SWAGGER_APPS", apps) \
        .mount("/etc/ledp", ledp)
    task = TaskDefinition("swaggertask")
    task.add_container(container)
    task.add_volume(ledp)
    return task

def haproxy_task(stackname, ec2s, ecr):
    tokens = []
    for ec2 in ec2s:
        tokens.append({ "Fn::GetAtt" : [ ec2.logical_id(), "PrivateIp" ]})
    ips = { "Fn::Join" : [ ",", tokens ]}
    ledp = Volume("ledp", "/etc/ledp")
    container = ContainerDefinition("haproxy", ecr + "/latticeengines/haproxy") \
        .mem_mb("768") \
        .publish_port(80, 80) \
        .publish_port(443, 443) \
        .publish_port(81, 81) \
        .publish_port(82, 82) \
        .publish_port(83, 83) \
        .set_logging({
        "LogDriver": "awslogs",
        "Options": {
            "awslogs-group": "ministack-%s" % stackname,
            "awslogs-region": { "Ref": "AWS::Region" },
            "awslogs-stream-prefix": "haproxy"
        }}) \
        .set_env("HOSTS", ips) \
        .mount("/etc/ledp", ledp)
    task = TaskDefinition("haproxytask")
    task.add_container(container)
    task.add_volume(ledp)

    for ec2 in ec2s:
        task.depends_on(ec2)
    return task

def provision_cli(args):
    provision(args.environment, args.stackname, args.tag, args.instancetype)

def provision(environment, stackname, tag, instance_type):
    global ALLOCATION
    ALLOCATION = load_allocation()

    config = AwsEnvironment(environment)
    client = boto3.client('cloudformation')
    check_stack_not_exists(client, stackname)

    create_log_group("ministack-%s" % stackname)

    subnet1 = config.private_subnet_1()
    subnet2 = config.private_subnet_2()
    subnet3 = config.private_subnet_3()

    params = [
        PARAM_VPC_ID.config(config.vpc()),
        PARAM_SUBNET_1.config(subnet1),
        PARAM_SUBNET_2.config(subnet2),
        PARAM_SUBNET_3.config(subnet3),
        PARAM_KEY_NAME.config(config.ec2_key()),
        PARAM_SECURITY_GROUP.config(config.ministack_sg()),
        PARAM_INSTANCE_TYPE.config(instance_type),
        PARAM_ENVIRONMENT.config(environment),
        PARAM_CAPACITY.config("0"),
        PARAM_MAX_CAPACITY.config("0"),
        PARAM_TARGET_GROUP.config(DUMMY_TGRP),
        PARAM_ECS_INSTANCE_PROFILE_NAME.config(config.ecs_instance_profile_name()),
        PARAM_ECS_INSTANCE_PROFILE_ARN.config(config.ecs_instance_profile_arn()),
        PARAM_ECS_INSTANCE_ROLE_NAME.config(config.ecs_instance_role_name()),
        PARAM_DOCKER_IMAGE_TAG.config(tag),
        PARAM_EFS.config(config.lpi_efs_id())
    ]

    response = client.create_stack(
        StackName=stackname,
        TemplateURL='https://%s/%s' % (config.s3_endpoint(), os.path.join(config.cf_bucket(), infra_stack_s3(stackname), 'template.json')),
        Parameters=params,
        TimeoutInMinutes=60,
        OnFailure='ROLLBACK',
        Capabilities=[
            'CAPABILITY_IAM',
        ],
        Tags=[
            {
                'Key': 'le-env',
                'Value': config.tag_le_env()
            },
            {
                'Key': 'le-product',
                'Value': 'lpi'
            },
            {
                'Key': 'le-stack',
                'Value': stackname
            }
        ]
    )
    print 'Got StackId: %s' % response['StackId']
    wait_for_stack_creation(client, stackname)

    ip = get_proxy_ip(stackname)
    consul = config.consul_server()
    write_to_stack(consul, environment, stackname, HAPROXY_KEY, ip)

def bootstrap_cli(args):
    bootstrap(args.environment, args.stackname, args.apps, args.profile, args.instances, args.tag, args.logdriver)

def bootstrap(environment, stackname, apps, profile, instances, tag, logdriver):
    global ALLOCATION
    ALLOCATION = load_allocation()

    config = AwsEnvironment(environment)

    consul = config.consul_server()
    ip = read_from_stack(consul, environment, stackname, HAPROXY_KEY)
    print "Retrieve HAProxy IP from consul: %s" % ip

    threads = []
    for app in apps.split(","):
        alloc = ALLOCATION[app]
        num_tasks = alloc['capacity'] if 'capacity' in alloc else 1
        num_tasks = instances if instances < num_tasks else num_tasks
        thread = CreateServiceThread(environment, stackname, app, num_tasks, ip, profile, tag, logdriver)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join(120)

    print "HAProxy IP from consul: %s" % ip

def tomcat_container(environment, stackname, app, ip, profile_file, tag, log_driver):
    alloc = ALLOCATION[app]
    config = AwsEnvironment(environment)
    container = Container("tomcat", "%s/latticeengines/%s:%s" % (config.ecr_registry(), app, tag))
    container.mem_mb(alloc["mem"])
    if "cpu" in alloc:
        container.cpu(alloc["cpu"])

    container.add_docker_label('stack', stackname)
    container.add_docker_label('app', app)

    if log_driver == 'awslogs':
        container.log("awslogs", {
            "awslogs-group": "ministack-%s" % stackname,
            "awslogs-region": config.aws_region(),
            "awslogs-stream-prefix": app
        })
    elif log_driver == 'splunk':
        container.log("splunk", {
            "splunk-url": config.splunk_url(),
            "splunk-token": config.splunk_token(),
            "splunk-index": "main",
            "splunk-sourcetype": "log4j",
            "splunk-format": "raw",
            "splunk-gzip": "true",
            "labels": "stack,app"
        })

    container.publish_port(8443, alloc["port"])
    container.hostname("%s-%s" % (stackname, app))
    container.ulimit("NOFILE", 2048, 2048)

    params = get_profile_vars(profile_file)
    params["LE_CLIENT_ADDRESS"] = ip
    params["HAPROXY_ADDRESS"] = ip

    protocol = "https"
    params["AWS_PRIVATE_LB"] = "%s://%s" % (protocol, ip)
    params["AWS_PUBLIC_LB"] = "%s://%s" % (protocol, ip)
    params["LE_STACK"] = stackname
    params["LE_ENVIRONMENT"] = environment
    params["CATALINA_OPTS"] = "-Xmx%dm -XX:ReservedCodeCacheSize=%dm" % (int(alloc["mem"] * 0.9), 256 if alloc["mem"] <= 1024 else 512)
    if 'QUARTZ_DB' not in params or params['QUARTZ_DB'] == '':
        params['QUARTZ_DB'] = "QuartzDB"
    for k, v in params.items():
        container.set_env(k, v)

    container = container.mount("/etc/ledp", "ledp") \
        .mount("/var/log/ledp", "ledpLog") \
        .mount("/etc/internaladdr.txt", "intAddr")

    return container

def ui_container(environment, stackname, app, ip, profile_file, tag):
    config = AwsEnvironment(environment)
    alloc = ALLOCATION[app]
    container = Container("express", "%s/latticeengines/express:%s" % (config.ecr_registry(), tag))
    container.mem_mb(alloc["mem"])
    if "cpu" in alloc:
        container.cpu(alloc["cpu"])

    container.add_docker_label('stack', stackname)
    container.add_docker_label('app', app)
    container.log("awslogs", {
        "awslogs-group": "ministack-%s" % stackname,
        "awslogs-region": config.aws_region(),
        "awslogs-stream-prefix": app
    })

    container.publish_port(3000, 3000)
    container.publish_port(3002, 3002)
    container.hostname("%s-%s" % (stackname, app))
    container.privileged()

    params = get_profile_vars(profile_file)
    params["LE_CLIENT_ADDRESS"] = ip
    params["HAPROXY_ADDRESS"] = ip

    # TODO: change to https
    protocol = "https"
    params["AWS_PRIVATE_LB"] = "%s://%s" % (protocol, ip)
    params["AWS_PUBLIC_LB"] = "%s://%s" % (protocol, ip)
    params["LE_STACK"] = stackname
    params["LE_ENVIRONMENT"] = environment
    for k, v in params.items():
        container.set_env(k, v)

    container = container.mount("/etc/ledp", "ledp") \
        .mount("/var/log/ledp", "ledpLog") \
        .mount("/etc/internaladdr.txt", "intAddr")

    return container

def get_proxy_ip(stackname):
    stack = boto3.resource('cloudformation').Stack(stackname)
    for output in stack.outputs:
        key = output['OutputKey']
        value = output['OutputValue']
        if "EC2Instance1" in key:
            print "Private IP for HAProxy is %s" % value
            return value

def teardown_cli(args):
    teardown(args.stackname, apps=args.apps, completely=args.completely)

def teardown(stackname, apps=None, completely=False):
    threads = []

    if (apps is None) or completely:
        apps = ALL_APPS

    app_list = apps.split(",")
    count = 0
    while count < len(app_list):
        batch_size = 0
        while batch_size < 6 and count < len(app_list):
            app = app_list[count]
            thread = DeleteServiceThread(stackname, app)
            thread.start()
            threads.append(thread)
            count += 1
            batch_size += 1
        for thread in threads:
            thread.join(120)
        time.sleep(20)
        threads = []

    if len(threads) > 0:
        for thread in threads:
            thread.join(120)
        time.sleep(20)

    if completely:
        client = boto3.client('cloudformation')
        teardown_stack(client, stackname)
        clean_log_group("ministack-%s" % stackname)

def load_allocation(alloction_json=None):
    if alloction_json is None:
        json_file = os.path.join(TEMPLATE_DIR, 'ministack', 'allocation.json')
    else:
        json_file = alloction_json
    with open(json_file) as f:
        return json.loads(f.read())

def get_profile_vars(profile):
    params = {}
    if profile is not None:
        with open(profile, 'r') as file:
            for line in file:
                line = line.strip().replace('\n', '')
                if len(line) > 0 and ('#' != line[0]):
                    key = line.split('=')[0]
                    value = line[len(key) + 1:]
                    params[key] = value
    return params

def infra_stack_s3(stackname):
    return _S3_CF_PATH + stackname

def parse_args():
    parser = argparse.ArgumentParser(description='ECS cluster CloudFormation cli')
    commands = parser.add_subparsers(help="commands")

    parser1 = commands.add_parser("template")
    parser1.add_argument('-e', dest='environment', type=str, default='devcluster', choices=['devcluster', 'qacluster','prodcluster'], help='environment')
    parser1.add_argument('-s', dest='stackname', type=str, required=True, help='the LE_STACK to be created')
    parser1.add_argument('-a', dest='apps', type=str, default=DEFAULT_APPS, help='comma separated list of swagger apps.')
    parser1.add_argument('-n', dest='instances', type=int, default="2", help='number of instances.')
    parser1.add_argument('-u', dest='upload', action='store_true', help='upload to S3')
    parser1.set_defaults(func=template_cli)

    parser1 = commands.add_parser("provision")
    parser1.add_argument('-e', dest='environment', type=str, default='devcluster', choices=['devcluster', 'qacluster','prodcluster'], help='environment')
    parser1.add_argument('-s', dest='stackname', type=str, required=True, help='the LE_STACK to be created')
    parser1.add_argument('-t', dest='tag', type=str, default='latest', help='docker image tag')
    parser1.add_argument('-i', dest='instancetype', type=str, default='r3.large', help='EC2 instance type')
    parser1.set_defaults(func=provision_cli)

    parser1 = commands.add_parser("bootstrap")
    parser1.add_argument('-e', dest='environment', type=str, default='devcluster', choices=['devcluster', 'qacluster','prodcluster'], help='environment')
    parser1.add_argument('-s', dest='stackname', type=str, required=True, help='the LE_STACK to be created')
    parser1.add_argument('-a', dest='apps', type=str, default=DEFAULT_APPS, help='comma separated list of apps to bootstrap.')
    parser1.add_argument('-n', dest='instances', type=int, default="2", help='number of instances.')
    parser1.add_argument('-t', dest='tag', type=str, default='latest', help='docker image tag')
    parser1.add_argument('-p', dest='profile', type=str, help='stack profile file')
    parser1.add_argument('-l', dest='logdriver', type=str, default='awslogs', help='log driver')
    parser1.set_defaults(func=bootstrap_cli)

    parser1 = commands.add_parser("teardown")
    parser1.add_argument('-s', dest='stackname', type=str, required=True, help='the LE_STACK to be created')
    parser1.add_argument('-a', dest='apps', type=str, help='comma separated list of apps to teardown.')
    parser1.add_argument('--include-infra', dest='completely', action="store_true", help='completely tear down: including infrastructure')
    parser1.set_defaults(func=teardown_cli)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
