"""
ECS stack for zookeeper
"""

import argparse
import boto3
import json
import os
import threading

from .consul import write_to_stack, read_from_stack
from .module.ecs import ContainerDefinition, TaskDefinition
from .module.parameter import *
from .module.stack import ECSStack, check_stack_not_exists, wait_for_stack_creation, teardown_stack
from .module.template import TEMPLATE_DIR
from ..conf import AwsEnvironment
from ..cw.logs import clean_internal as clean_log_group
from ..cw.logs import create_internal as create_log_group
from ..ecs.container import Container
from ..ecs.manage import register_task, deregister_task, create_service, delete_service
from ..ecs.volume import Volume as ECSVolume
from ..elb.targetgroup import DUMMY_TGRP

_S3_CF_PATH='cloudformation/ministack/'

PARAM_DOCKER_IMAGE_TAG=Parameter("DockerImageTag", "Docker image tag to be deployed", default="latest")
PARAM_EFS = Parameter("Efs", "EFS Id")

ALL_APPS="pls,admin,matchapi,scoringapi,oauth2,playmaker,eai,metadata,scoring,modeling,dataflowapi,workflowapi,quartz,modelquality,propdata,dellebi"
DEFAULT_APPS="pls,admin,matchapi,scoringapi,oauth2,playmaker,eai,metadata,scoring,modeling,dataflowapi,workflowapi"
ALLOCATION = {}
HAPROXY_KEY="HAProxy"

class CreateServiceThread (threading.Thread):
    def __init__(self, environment, stackname, app, ecr, ip, profile, region):
        threading.Thread.__init__(self)
        self.threadID = "%s-%s" % (stackname, app)
        self.environment = environment
        self.stackname = stackname
        self.app = app
        self.ecr = ecr
        self.ip = ip
        self.profile = profile
        self.region = region

    def run(self):
        container = tomcat_container(self.environment, self.stackname, self.ecr, self.app, self.ip, self.profile, region=self.region)
        ledp = ECSVolume("ledp", "/etc/ledp")
        scoringcache = ECSVolume("scoringcache", "/mnt/efs/scoringapi")
        task = "%s-%s" % (self.stackname, self.app)
        register_task(task, [container], [ledp, scoringcache])
        create_service(self.stackname, self.app, task, 1)


class DeleteServiceThread (threading.Thread):
    def __init__(self, stackname, app):
        threading.Thread.__init__(self)
        self.threadID = "%s-%s" % (stackname, app)
        self.stackname = stackname
        self.app = app

    def run(self):
        delete_service(self.stackname, self.app)
        deregister_task("%s-%s" % (self.stackname, self.app))

def main():
    args = parse_args()
    args.func(args)

def template_cli(args):
    template(args.environment, args.stackname, args.instances, args.apps, upload=args.upload)

def template(environment, stackname, instances, apps, upload=False):
    infra_stack = create_infra_template(stackname, instances, apps)
    if upload:
        infra_stack.validate()
        infra_stack.upload(environment, infra_stack_s3(stackname))
    else:
        print infra_stack.json()
        infra_stack.validate()

def create_infra_template(stackname, instances, apps):
    stack = ECSStack("AWS CloudFormation template for mini-stack infrastructure.", use_asgroup=False, instances=instances, efs=PARAM_EFS)
    stack.add_params([PARAM_EFS, PARAM_DOCKER_IMAGE_TAG])

    task = swagger_task(stackname, apps)
    stack.add_resource(task)
    stack.add_service("swagger", task, capacity=1)

    task = haproxy_task(stackname, stack.get_ec2s())
    stack.add_resource(task)
    haproxy = stack.create_service("haproxy", task, capacity=instances)
    stack.add_resource(haproxy)

    return stack

def swagger_task(stackname, apps):
    container = ContainerDefinition("httpd", { "Fn::Join" : [ "", [
        { "Fn::FindInMap" : [ "Environment2Props", PARAM_ENVIRONMENT.ref(), "EcrRegistry" ] },
        "/latticeengines/swagger" ] ]}) \
        .mem_mb("256") \
        .publish_port(80, 8080) \
        .set_logging({
        "LogDriver": "awslogs",
        "Options": {
            "awslogs-group": "ministack-%s" % stackname,
            "awslogs-region": { "Ref": "AWS::Region" }
        }}) \
        .set_env("SWAGGER_APPS", apps)
    task = TaskDefinition("swaggertask")
    task.add_container(container)
    return task

def haproxy_task(stackname, ec2s):
    tokens = []
    for ec2 in ec2s:
        tokens.append({ "Fn::GetAtt" : [ ec2.logical_id(), "PrivateIp" ]})
    ips = { "Fn::Join" : [ ",", tokens ]}

    container = ContainerDefinition("haproxy", { "Fn::Join" : [ "", [
        { "Fn::FindInMap" : [ "Environment2Props", PARAM_ENVIRONMENT.ref(), "EcrRegistry" ] },
        "/latticeengines/haproxy" ] ]}) \
        .mem_mb("768") \
        .publish_port(80, 80) \
        .publish_port(81, 81) \
        .set_logging({
        "LogDriver": "awslogs",
        "Options": {
            "awslogs-group": "ministack-%s" % stackname,
            "awslogs-region": { "Ref": "AWS::Region" }
        }}) \
        .set_env("HOSTS", ips)
    task = TaskDefinition("haproxytask")
    task.add_container(container)

    for ec2 in ec2s:
        task.depends_on(ec2)
    return task

def provision_cli(args):
    provision(args.environment, args.stackname, args.tag, args.consul)

def provision(environment, stackname, tag, consul):
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
        PARAM_SECURITY_GROUP.config(config.tomcat_sg()),
        PARAM_INSTANCE_TYPE.config('m4.xlarge'),
        PARAM_ENVIRONMENT.config(environment),
        PARAM_CAPACITY.config("0"),
        PARAM_MAX_CAPACITY.config("0"),
        PARAM_TARGET_GROUP.config(DUMMY_TGRP),
        PARAM_ECS_INSTANCE_PROFILE_NAME.config(config.ecs_instance_profile_name()),
        PARAM_ECS_INSTANCE_PROFILE_ARN.config(config.ecs_instance_profile_arn()),
        PARAM_DOCKER_IMAGE_TAG.config(tag),
        PARAM_EFS.config(config.lpi_efs_id())
    ]

    response = client.create_stack(
        StackName=stackname,
        TemplateURL='https://s3.amazonaws.com/%s' % os.path.join(config.cf_bucket(), infra_stack_s3(stackname), 'template.json'),
        Parameters=params,
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

    ip = get_proxy_ip(stackname)
    if consul is not None:
        write_to_stack(consul, environment, stackname, HAPROXY_KEY, ip)

def bootstrap_cli(args):
    bootstrap(args.environment, args.stackname, args.ip, args.apps, args.profile, consul=args.consul)

def bootstrap(environment, stackname, ip, apps, profile, consul=None, region="us-east-1"):
    global ALLOCATION
    ALLOCATION = load_allocation()

    config = AwsEnvironment(environment)
    ecr_url = config.ecr_registry()

    if consul is not None:
        ip = read_from_stack(consul, environment, stackname, HAPROXY_KEY)
        print "Retrieve HAProxy IP from consul: %s" % ip

    threads = []
    for app in apps.split(","):
        thread = CreateServiceThread(environment, stackname, app, ecr_url, ip, profile, region)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join(120)

def tomcat_container(environment, stackname, ecr_url, app, ip, profile_file, region="us-east-1"):
    alloc = ALLOCATION[app]
    container = Container("tomcat", "%s/latticeengines/%s" % (ecr_url, app))
    container.mem_mb(alloc["mem"])
    if "cpu" in alloc:
        container.cpu(alloc["cpu"])
    container.log("awslogs", {
        "awslogs-group": "ministack-%s" % stackname,
        "awslogs-region": region
    })
    container.publish_port(8080, alloc["port"])

    params = get_profile_vars(profile_file)
    params["LE_CLIENT_ADDRESS"] = ip
    params["HAPROXY_ADDRESS"] = ip

    # TODO: change to https
    protocol = params["HTTP_PROTOCOL"] if "HTTP_PROTOCOL" in params else "http"
    params["AWS_PLS_ADDRESS"] = "%s://%s" % (protocol, ip)
    params["AWS_ADMIN_ADDRESS"] = "%s://%s" % (protocol, ip)
    params["AWS_MICROSERVICE_ADDRESS"] = "%s://%s" % (protocol, ip)
    params["AWS_SCORINGAPI_ADDRESS"] = "%s://%s" % (protocol, ip)
    params["AWS_MATCHAPI_ADDRESS"] = "%s://%s" % (protocol, ip)
    params["AWS_OAUTH_ADDRESS"] = "%s://%s/oauth2" % (protocol, ip)
    params["AWS_PLAYMAKER_ADDRESS"] = "%s://%s/playmaker" % (protocol, ip)

    params["LE_SWLIB_DISABLED"] = "true"
    params["LE_STACK"] = stackname
    params["LE_ENVIRONMENT"] = environment
    params["CATALINA_OPTS"] = "-Xmx%dm" % (int(alloc["mem"] * 0.9))
    for k, v in params.items():
        container.set_env(k, v)

    container = container.mount("/etc/ledp", "ledp").mount("/var/cache/scoringapi", "scoringcache")

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

    for app in apps.split(","):
        thread = DeleteServiceThread(stackname, app)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join(120)

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
                    value = line.split('=')[1]
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
    parser1.add_argument('-n', dest='instances', type=int, default="1", help='number of instances.')
    parser1.add_argument('-u', dest='upload', action='store_true', help='upload to S3')
    parser1.set_defaults(func=template_cli)

    parser1 = commands.add_parser("provision")
    parser1.add_argument('-e', dest='environment', type=str, default='devcluster', choices=['devcluster', 'qacluster','prodcluster'], help='environment')
    parser1.add_argument('-s', dest='stackname', type=str, required=True, help='the LE_STACK to be created')
    parser1.add_argument('-t', dest='tag', type=str, default='latest', help='docker image tag')
    parser1.add_argument('-c', dest='consul', type=str, help='consul server address')
    parser1.set_defaults(func=provision_cli)

    parser1 = commands.add_parser("bootstrap")
    parser1.add_argument('-e', dest='environment', type=str, default='devcluster', choices=['devcluster', 'qacluster','prodcluster'], help='environment')
    parser1.add_argument('-s', dest='stackname', type=str, required=True, help='the LE_STACK to be created')
    parser1.add_argument('-a', dest='apps', type=str, default=DEFAULT_APPS, help='comma separated list of apps to bootstrap.')
    parser1.add_argument('-i', dest='ip', type=str, help='IP of HAProxy. need either ip or a consul address')
    parser1.add_argument('-c', dest='consul', type=str, help='consul server address. need either ip or a consul address')
    parser1.add_argument('-t', dest='tag', type=str, default='latest', help='docker image tag')
    parser1.add_argument('-p', dest='profile', type=str, help='stack profile file')
    parser1.set_defaults(func=bootstrap_cli)

    parser1 = commands.add_parser("teardown")
    parser1.add_argument('-s', dest='stackname', type=str, required=True, help='the LE_STACK to be created')
    parser1.add_argument('-a', dest='apps', type=str, help='comma separated list of apps to teardown.')
    parser1.add_argument('-c', dest='consul', type=str, help='consul server address')
    parser1.add_argument('--include-infra', dest='completely', action="store_true", help='completely tear down: including infrastructure')
    parser1.set_defaults(func=teardown_cli)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()