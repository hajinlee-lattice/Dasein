import argparse
import boto3
import json
import os
from boto3.s3.transfer import S3Transfer

from .kafka_profile import KafkaProfile, DEFAULT_PROFILE
from .module.autoscaling import AutoScalingGroup, LaunchConfiguration
from .module.ec2 import _ec2_params
from .module.ecs import ECSCluster, ECSService, TaskDefinition, ContainerDefinition, Volume
from .module.elb import ElasticLoadBalancer
from .module.iam import ECSServiceRole, InstanceProfile, AccessKey
from .module.stack import Stack, teardown_stack
from .module.stack import check_stack_not_exists, wait_for_stack_creation, S3_BUCKET
from .module.template import TEMPLATE_DIR

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

        app_file = os.path.join(TEMPLATE_DIR, "kafka", "app.py")
        print 'uploading app.py to %s' % (os.path.join("https://s3.amazonaws.com", S3_BUCKET, _S3_CF_PATH, 'app.py') + ' ..')
        client = boto3.client('s3')
        transfer = S3Transfer(client)
        transfer.upload_file(app_file, S3_BUCKET, os.path.join(_S3_CF_PATH, 'app.py'))
        print 'done.'

    else:
        print stack.json()
        stack.validate()

def template_internal():
    stack = Stack("AWS CloudFormation template for Kafka ECS container instances.")
    stack.add_params(extra_param()).add_params(_ec2_params())

    ecscluster = ECSCluster("cluster")
    stack.add_resource(ecscluster)

    # elb9092 = ElasticLoadBalancer("lb9092").listen("9092")
    elb9022 = ElasticLoadBalancer("lb9022").listen("9022", protocol="http")
    elb9023 = ElasticLoadBalancer("lb9023").listen("9023", protocol="http")
    stack.add_resources([elb9022, elb9023])

    # Auto Scaling group
    asgroup = AutoScalingGroup("ScalingGroup")
    ec2role = ECSServiceRole("EC2Role")
    instanceprofile = InstanceProfile("EC2InstanceProfile", ec2role)
    launchconfig = LaunchConfiguration("ContainerPool", ecscluster, asgroup, instanceprofile)
    asgroup.add_pool(launchconfig)
    stack.add_resources(create_as_group(ecscluster, [elb9022, elb9023]))

    # Docker service
    # ecsrole = ECSServiceRole("ECSServiceRole")
    dockers = docker_resources(ecscluster)
    stack.add_resources(dockers)

    # Outputs
    outputs = {
        # "BrokerLoadBalancer": {
        #     "Description" : "URL for Schema Registry's load balancer",
        #     "Value" : { "Fn::GetAtt" : [ elb9092.logical_id(), "DNSName" ]}
        # },
        "SchemaRegistryLoadBalancer": {
            "Description" : "URL for Schema Registry's load balancer",
            "Value" : { "Fn::GetAtt" : [ elb9022.logical_id(), "DNSName" ]}
        },
        "KafkaRESTLoadBalancer": {
            "Description" : "URL for Schema Registry's load balancer",
            "Value" : { "Fn::GetAtt" : [ elb9023.logical_id(), "DNSName" ]}
        }
    }
    stack.add_ouputs(outputs)

    return stack

def provision(args):
    if args.profile is None:
        profile = DEFAULT_PROFILE
    else:
        profile = KafkaProfile(args.profile)

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
                'ParameterValue': profile.instance_type()
            },
            {
                'ParameterKey': 'DesiredCapacity',
                'ParameterValue': profile.num_instances()
            },
            {
                'ParameterKey': 'MaxSize',
                'ParameterValue': profile.max_instances()
            },
            {
                'ParameterKey': 'ZookeeperHosts',
                'ParameterValue': args.zkhosts
            },
            {
                'ParameterKey': 'PrimaryBrokers',
                'ParameterValue': profile.num_pub_bkrs()
            },
            {
                'ParameterKey': 'SecondaryBrokers',
                'ParameterValue': profile.num_pri_bkrs()
            },
            {
                'ParameterKey': 'BrokerMemory',
                'ParameterValue': profile.bkr_mem()
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
    get_elbs(args.stackname)

def describe(args):
    stack = boto3.resource('cloudformation').Stack(args.stackname)
    print stack

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

def docker_resources(ecscluster):
    pri_bkr, pri_bkr_task= primary_broker_service(ecscluster)
    sec_bkr, sec_bkr_task = secondary_broker_service(ecscluster)
    sr, sr_task = schema_registry_service(ecscluster, [pri_bkr, sec_bkr])

    return pri_bkr, pri_bkr_task, \
           sec_bkr, sec_bkr_task, \
           sr, sr_task

def primary_broker_service(ecscluster):
    intaddr = Volume("internaladdr", "/etc/internaladdr.txt")
    extaddr= Volume("externaladdr", "/etc/externaladdr.txt")
    container = ContainerDefinition("bkr", os.path.join(_ECR_REPO, "latticeengines/kafka")) \
        .mem_mb({ "Ref" : "BrokerMemory" }).publish_port(9092, 9092) \
        .set_env("ZK_HOSTS", { "Ref" : "ZookeeperHosts" }) \
        .set_logging({
            "LogDriver": "awslogs",
            "Options": {
                "awslogs-group": "docker-kafka-broker",
                "awslogs-region": { "Ref": "AWS::Region" }
            }
        }) \
        .mount("/etc/internaladdr.txt", intaddr)\
        .mount("/etc/externaladdr.txt", extaddr)

    task = TaskDefinition("BrokerTask")
    task.add_container(container).add_volume(intaddr).add_volume(extaddr)

    service = ECSService("Broker", ecscluster, task, { "Ref": "PrimaryBrokers" })
    return service, task

def secondary_broker_service(ecscluster):
    intaddr = Volume("internaladdr", "/etc/internaladdr.txt")
    extaddr= Volume("externaladdr", "/etc/externaladdr.txt")
    container = ContainerDefinition("bkr", os.path.join(_ECR_REPO, "latticeengines/kafka")) \
        .mem_mb({ "Ref" : "BrokerMemory" }).publish_port(9093, 9093) \
        .set_env("BROKER_PORT", "9093") \
        .set_env("ZK_HOSTS", { "Ref" : "ZookeeperHosts" }) \
        .set_logging({
            "LogDriver": "awslogs",
            "Options": {
                "awslogs-group": "docker-kafka-broker",
                "awslogs-region": { "Ref": "AWS::Region" }
            }
        }) \
        .mount("/etc/internaladdr.txt", intaddr) \
        .mount("/etc/externaladdr.txt", extaddr)
    task = TaskDefinition("SecondaryBrokerTask")
    task.add_container(container).add_volume(intaddr).add_volume(extaddr)

    service = ECSService("SecondaryBroker", ecscluster, task, { "Ref": "SecondaryBrokers" })
    return service, task

def schema_registry_service(ecscluster, brokers):
    intaddr = Volume("internaladdr", "/etc/internaladdr.txt")
    extaddr= Volume("externaladdr", "/etc/externaladdr.txt")
    container = ContainerDefinition("sr", os.path.join(_ECR_REPO, "latticeengines/kafka-schema-registry")) \
        .mem_mb(1024).publish_port(9022, 9022) \
        .set_env("ZK_HOSTS", { "Ref" : "ZookeeperHosts" }) \
        .set_logging({
            "LogDriver": "awslogs",
            "Options": {
                "awslogs-group": "docker-schema-registry",
                "awslogs-region": { "Ref": "AWS::Region" }
            }
        }) \
        .mount("/etc/internaladdr.txt", intaddr) \
        .mount("/etc/externaladdr.txt", extaddr) \
        .hostname("schema-registry")

    container2 = ContainerDefinition("kr", os.path.join(_ECR_REPO, "latticeengines/kafka-rest")) \
        .mem_mb(512).publish_port(9023, 9023) \
        .set_env("ZK_HOSTS", { "Ref" : "ZookeeperHosts" }) \
        .set_env("SR_PROXY", "http://%s:9022" % container.get_name()) \
        .set_logging({
            "LogDriver": "awslogs",
            "Options": {
                "awslogs-group": "docker-rest",
                "awslogs-region": { "Ref": "AWS::Region" }
            }
        }) \
        .link(container)

    task = TaskDefinition("SchemaRegistryTask")
    task.add_container(container).add_container(container2)\
        .add_volume(intaddr).add_volume(extaddr)

    service = ECSService("SchemaRegistry", ecscluster, task, 2)
    for broker_service in brokers:
        service.depends_on(broker_service)
    return service, task

def get_elbs(stackname):
    stack = boto3.resource('cloudformation').Stack(stackname)
    for output in stack.outputs:
        key = output['OutputKey']
        value = output['OutputValue']
        if 'LoadBalancer' in key:
            print "DNS name for %s is %s" % (key, value)

def instance_name(idx):
    return "EC2Instance%d" % (idx + 1)

def discover_metadata(key):
    assert isinstance(key, AccessKey)

    json_file = os.path.join(TEMPLATE_DIR, 'kafka', 'ec2_metadata.json')
    with open(json_file) as f:
        text = f.read().replace('{{APP_PATH}}', os.path.join(S3_BUCKET, _S3_CF_PATH, "app.py"))\
                .replace('{{BUCKET}}', S3_BUCKET).replace('{{CFN_KEYS}}', key.logical_id())
    return json.loads(text)

def extra_param():
    json_file = os.path.join(TEMPLATE_DIR, 'kafka', 'params.json')
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
    parser1.add_argument('-z', dest='zkhosts', type=str, required=True, help='zk connection string')
    parser1.add_argument('-p', dest='profile', type=str, help='profile file')
    parser1.set_defaults(func=provision)

    parser1 = commands.add_parser("describe")
    parser1.add_argument('-s', dest='stackname', type=str, default='kafka', help='stack name')
    parser1.set_defaults(func=describe)

    parser1 = commands.add_parser("teardown")
    parser1.add_argument('-s', dest='stackname', type=str, default='kafka', help='stack name')
    parser1.set_defaults(func=teardown)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
