import argparse
import boto3
import json
import os

from .autoscaling import AutoScalingGroup, LaunchConfiguration
from .ec2 import _ec2_params
from .ecs import ECSCluster, ECSService, TaskDefinition, ContainerDefinition
from .elb import ElasticLoadBalancer, ElbListener, ElbHealthCheck
from .iam import ECSServiceRole, InstanceProfile
from .stack import Stack, teardown_stack
from .stack import check_stack_not_exists, wait_for_stack_creation, S3_BUCKET
from .template import TEMPLATE_DIR

_S3_CF_PATH='cloudformation/kafka'
_EC2_PEM = '~/aws.pem'
_ECR_REPO="894091243412.dkr.ecr.us-east-1.amazonaws.com"

def main():
    args = parse_args()
    args.func(args)

def template(args):
    stack = template_internal()
    if args.upload:
        stack.validate()
        stack.upload(_S3_CF_PATH)
    else:
        print stack.json()
        stack.validate()

def template_internal():
    stack = Stack("AWS CloudFormation template for Kafka ECS container instances.")
    stack.add_params(extra_param()).add_params(_ec2_params())

    ecscluster = ECSCluster("ECSCluster")
    stack.add_resource(ecscluster)

    healthcheck5000 = ElbHealthCheck(5000, healthy_threshold=3, timeout=2, unhealthy_threshold=2, interval=10)
    elb5000 = ElasticLoadBalancer("LoadBalancer5000").add_listener(ElbListener("5000")).add_healthcheck(healthcheck5000)
    elb9092 = ElasticLoadBalancer("LoadBalancer9092").add_listener(ElbListener("9092"))
    elb9022 = ElasticLoadBalancer("LoadBalancer9022").add_listener(ElbListener("9022"))
    elb9023 = ElasticLoadBalancer("LoadBalancer9023").add_listener(ElbListener("9023"))
    stack.add_resources([elb5000, elb9092, elb9022, elb9023])

    # Auto Scaling group
    asgroup = AutoScalingGroup("ScalingGroup")
    role = ECSServiceRole("EC2Role")
    instanceprofile = InstanceProfile("EC2InstanceProfile", role)
    launchconfig = LaunchConfiguration("ContainerPool", ecscluster, asgroup, instanceprofile)
    asgroup.add_pool(launchconfig)
    stack.add_resources(create_as_group(ecscluster, [elb5000, elb9092, elb9022, elb9023]))

    # Docker service
    stack.add_resources(discover_service(ecscluster, role, elb5000))

    return stack

def provision(args):
    client = boto3.client('cloudformation')
    check_stack_not_exists(client, args.stackname)
    response = client.create_stack(
        StackName=args.stackname,
        TemplateURL='https://s3.amazonaws.com/%s' % os.path.join(S3_BUCKET, _S3_CF_PATH, 'template.json'),
        Parameters=[
            {
                'ParameterKey': 'SubnetId1',
                'ParameterValue': 'subnet-7550002d'
            },
            {
                'ParameterKey': 'SubnetId2',
                'ParameterValue': 'subnet-310d5a1b'
            },
            {
                'ParameterKey': 'TrustedIPZone',
                'ParameterValue': '0.0.0.0/0'
            },
            {
                'ParameterKey': 'KeyName',
                'ParameterValue': 'ysong-east'
            },
            {
                'ParameterKey': 'SecurityGroupId',
                'ParameterValue': 'sg-b3dbb0c8'
            },
            {
                'ParameterKey': 'InstanceType',
                'ParameterValue': 't2.medium'
            },
            {
                'ParameterKey': 'DesiredCapacity',
                'ParameterValue': '2'
            },
            {
                'ParameterKey': 'MaxSize',
                'ParameterValue': '5'
            }
        ],
        TimeoutInMinutes=60,
        OnFailure='ROLLBACK',
        Capabilities=[
            'CAPABILITY_IAM',
        ],
        Tags=[
            {
                'Key': 'com.lattice-engines.cluster.name',
                'Value': args.stackname
            },
            {
                'Key': 'com.lattice-engines.cluster.type',
                'Value': 'ecs'
            },
        ]
    )
    print 'Got StackId: %s' % response['StackId']
    wait_for_stack_creation(client, args.stackname)

def bootstrap(args):
   pass

def teardown(args):
    client = boto3.client('cloudformation')
    teardown_stack(client, args.stackname)


def create_as_group(ecscluster, elbs):
    asgroup = AutoScalingGroup("ScalingGroup")
    role = ECSServiceRole("EC2Role")
    instanceprofile = InstanceProfile("EC2InstanceProfile", role)
    launchconfig = LaunchConfiguration("ContainerPool", ecscluster, asgroup, instanceprofile)
    asgroup.add_pool(launchconfig)
    asgroup.attach_elbs(elbs)
    return asgroup, role, instanceprofile, launchconfig

def discover_service(ecscluster, ecsrole, elb5000):
    container = ContainerDefinition("discover", os.path.join(_ECR_REPO, "latticeengines/discover"))\
        .cpu(50).mem_mb(64).publish_port(5000, 5000)
    task = TaskDefinition("DiscoverTask")
    task.add_container(container)

    service = ECSService("DiscoverService", ecscluster, task, ecsrole, 2)
    service.add_elb(elb5000, container, 5000)
    return service, task

def instance_name(idx):
    return "EC2Instance%d" % (idx + 1)

def extra_param():
    json_file = os.path.join(TEMPLATE_DIR, 'ecs', 'params.json')
    with open(json_file) as f:
        text = f.read()
        return json.loads(text)

def parse_args():
    parser = argparse.ArgumentParser(description='Kafka ECS CloudFormation management')
    commands = parser.add_subparsers(help="commands")

    parser1 = commands.add_parser("template")
    parser1.add_argument('-u', dest='upload', action='store_true', help='upload to S3')
    parser1.set_defaults(func=template)

    parser1 = commands.add_parser("provision")
    parser1.add_argument('-s', dest='stackname', type=str, default='kafka', help='stack name')
    parser1.set_defaults(func=provision)

    parser1 = commands.add_parser("bootstrap")
    parser1.add_argument('-s', dest='stackname', type=str, default='kafka', help='stack name')
    parser1.set_defaults(func=bootstrap)

    parser1 = commands.add_parser("teardown")
    parser1.add_argument('-s', dest='stackname', type=str, default='kafka', help='stack name')
    parser1.set_defaults(func=teardown)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
