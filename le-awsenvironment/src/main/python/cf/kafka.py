import argparse
import boto3
import json
import os

from .kafka_profile import KafkaProfile, DEFAULT_PROFILE
from .module.autoscaling import AutoScalingGroup, ECSLaunchConfiguration
from .module.ec2 import _ec2_params
from .module.ecs import ECSCluster, ECSService, TaskDefinition, ContainerDefinition, Volume
from .module.elb import ElasticLoadBalancer
from .module.iam import ECSServiceRole, InstanceProfile, AccessKey
from .module.stack import Stack, teardown_stack, check_stack_not_exists, wait_for_stack_creation, S3_BUCKET
from .module.template import TEMPLATE_DIR

_S3_CF_PATH='cloudformation/kafka'
_EC2_PEM = '~/aws.pem'
_ECR_REPO="894091243412.dkr.ecr.us-east-1.amazonaws.com"

def main():
    args = parse_args()
    args.func(args)

def template_cli(args):
    template(args.upload)

def template(upload):
    stack = create_template()
    if upload:
        stack.validate()
        stack.upload(_S3_CF_PATH)
    else:
        print stack.json()
        stack.validate()

def create_template():
    stack = Stack("AWS CloudFormation template for Kafka ECS container instances.")
    stack.add_params(extra_param()).add_params(_ec2_params())

    ecscluster = ECSCluster("cluster")
    stack.add_resource(ecscluster)

    elb9092 = ElasticLoadBalancer("lb9092").listen("9092")
    elb9022 = ElasticLoadBalancer("lb9022").listen("9022", protocol="http")
    elb9023 = ElasticLoadBalancer("lb9023").listen("9023", protocol="http")
    elb9024 = ElasticLoadBalancer("lb9024").listen("9024", protocol="http")
    stack.add_resources([elb9092, elb9022, elb9023, elb9024])

    # Auto Scaling group
    asgroup, ec2role, instanceprofile, launchconfig = create_as_group(ecscluster, [elb9092, elb9022, elb9023, elb9024])
    stack.add_resources([asgroup, ec2role, instanceprofile, launchconfig])

    # Docker service
    # ecsrole = ECSServiceRole("ECSServiceRole")
    dockers = docker_resources(ecscluster, asgroup, elb9092, elb9022)
    stack.add_resources(dockers)

    # Outputs
    outputs = {
        "BrokerLoadBalancer": {
            "Description" : "URL for Brokers' load balancer",
            "Value" : { "Fn::GetAtt" : [ elb9092.logical_id(), "DNSName" ]}
        },
        "SchemaRegistryLoadBalancer": {
            "Description" : "URL for Schema Registry's load balancer",
            "Value" : { "Fn::GetAtt" : [ elb9022.logical_id(), "DNSName" ]}
        },
        "KafkaConnectLoadBalancer": {
            "Description" : "URL for Kafka Connect's load balancer",
            "Value" : { "Fn::GetAtt" : [ elb9024.logical_id(), "DNSName" ]}
        }
    }
    stack.add_ouputs(outputs)

    return stack

def provision_cli(args):
    return provision(args.stackname, args.zkhosts, profile=args.profile)

def provision(stackname, zkhosts, profile=None):
    if profile is None:
        profile = DEFAULT_PROFILE
    else:
        profile = KafkaProfile(profile)

    client = boto3.client('cloudformation')
    check_stack_not_exists(client, stackname)
    response = client.create_stack(
        StackName=stackname,
        TemplateURL='https://s3.amazonaws.com/%s' % os.path.join(S3_BUCKET, _S3_CF_PATH, 'template.json'),
        Parameters=[
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
                'ParameterValue': zkhosts
            },
            {
                'ParameterKey': 'Brokers',
                'ParameterValue': profile.num_brokers()
            },
            {
                'ParameterKey': 'BrokerMemory',
                'ParameterValue': profile.broker_mem()
            },
            {
                'ParameterKey': 'SchemaRegistries',
                'ParameterValue': profile.num_registries()
            },
            {
                'ParameterKey': 'SchemaRegistryMemory',
                'ParameterValue': profile.registry_mem()
            },
            {
                'ParameterKey': 'ConnectWorkers',
                'ParameterValue': profile.num_workers()
            },
            {
                'ParameterKey': 'ConnectWorkerMemory',
                'ParameterValue': profile.worker_mem()
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
    return get_elbs(stackname)

def describe(args):
    stack = boto3.resource('cloudformation').Stack(args.stackname)
    print stack

def teardown_cli(args):
    teardown(args.stackname)

def teardown(stackname):
    client = boto3.client('cloudformation')
    teardown_stack(client, stackname)

def create_as_group(ecscluster, elbs):
    asgroup = AutoScalingGroup("ScalingGroup")
    role = ECSServiceRole("EC2Role")
    instanceprofile = InstanceProfile("EC2InstanceProfile", role)
    launchconfig = ECSLaunchConfiguration("ContainerPool", ecscluster, asgroup, instanceprofile)
    asgroup.add_pool(launchconfig)
    asgroup.attach_elbs(elbs)
    return asgroup, role, instanceprofile, launchconfig

def docker_resources(ecscluster, asgroup, elb9092, elb9022):
    bkr, bkr_task= broker_service(ecscluster, asgroup)
    sr, sr_task = schema_registry_service(ecscluster, bkr)
    conn, conn_task = kafka_connect_service(ecscluster, elb9092, elb9022, sr)
    kr, kr_task = kafka_rest_service(ecscluster, elb9022, sr)
    return bkr, bkr_task, sr, sr_task, conn, conn_task, kr, kr_task

def broker_service(ecscluster, asgroup):
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

    service = ECSService("Broker", ecscluster, task, { "Ref": "Brokers" }).depends_on(asgroup)
    return service, task

def schema_registry_service(ecscluster, broker):
    intaddr = Volume("internaladdr", "/etc/internaladdr.txt")
    extaddr= Volume("externaladdr", "/etc/externaladdr.txt")
    container = ContainerDefinition("sr", os.path.join(_ECR_REPO, "latticeengines/schema-registry")) \
        .mem_mb({ "Ref": "SchemaRegistryMemory" }).publish_port(9022, 9022) \
        .set_env("ZK_HOSTS", { "Ref" : "ZookeeperHosts" }) \
        .set_logging({
            "LogDriver": "awslogs",
            "Options": {
                "awslogs-group": "docker-schema-registry",
                "awslogs-region": { "Ref": "AWS::Region" }
            }
        }) \
        .mount("/etc/internaladdr.txt", intaddr) \
        .mount("/etc/externaladdr.txt", extaddr)

    task = TaskDefinition("SchemaRegistryTask")
    task.add_container(container).add_volume(intaddr).add_volume(extaddr)

    service = ECSService("SchemaRegistry", ecscluster, task, { "Ref": "SchemaRegistries" }).depends_on(broker)
    return service, task

def kafka_rest_service(ecscluster, elb9022, sr):
    container = ContainerDefinition("kr", os.path.join(_ECR_REPO, "latticeengines/kafka-rest")) \
        .mem_mb(1024).publish_port(9023, 9023) \
        .set_env("ZK_HOSTS", { "Ref" : "ZookeeperHosts" }) \
        .set_env("SR_ADDRESS",
                 {"Fn::Join" : [ "", [
                     "http://", {"Fn::GetAtt": [ elb9022.logical_id(), "DNSName" ] } , ":9022"
                 ]]})

    task = TaskDefinition("KafkaRESTTask")
    task.add_container(container)

    service = ECSService("KafkaREST", ecscluster, task, 2).depends_on(sr)
    return service, task

def kafka_connect_service(ecscluster, elb9092, elb9022, sr):
    container = ContainerDefinition("connect", os.path.join(_ECR_REPO, "latticeengines/kafka-connect")) \
        .mem_mb( { "Ref": "ConnectWorkerMemory" } ).publish_port(9024, 9024) \
        .set_env("BOOTSTRAP_SERVERS", { "Fn::Join" : [ "", [
            "http://", { "Fn::GetAtt": [ elb9092.logical_id(), "DNSName" ] } , ":9092"] ]}) \
        .set_env("SR_ADDRESS", { "Fn::Join" : [ "", [
            "http://", { "Fn::GetAtt": [ elb9022.logical_id(), "DNSName" ] } , ":9022"] ]}) \
        .set_logging({
            "LogDriver": "awslogs",
            "Options": {
                "awslogs-group": "docker-kafka-connect",
                "awslogs-region": { "Ref": "AWS::Region" }
            }
        })

    task = TaskDefinition("ConnectWorkerTask")
    task.add_container(container)

    service = ECSService("ConnectWorker", ecscluster, task, { "Ref": "ConnectWorkers" }).depends_on(sr)
    return service, task

def get_elbs(stackname):
    stack = boto3.resource('cloudformation').Stack(stackname)
    elbs = {}
    for output in stack.outputs:
        key = output['OutputKey']
        value = output['OutputValue']
        if 'LoadBalancer' in key:
            elbs[key] = value
            print "DNS name for %s is %s" % (key, value)
    return elbs

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
    parser1.set_defaults(func=template_cli)

    parser1 = commands.add_parser("provision")
    parser1.add_argument('-s', dest='stackname', type=str, default='kafka', help='stack name')
    parser1.add_argument('-z', dest='zkhosts', type=str, required=True, help='zk connection string')
    parser1.add_argument('-p', dest='profile', type=str, help='profile file')
    parser1.set_defaults(func=provision_cli)

    parser1 = commands.add_parser("describe")
    parser1.add_argument('-s', dest='stackname', type=str, default='kafka', help='stack name')
    parser1.set_defaults(func=describe)

    parser1 = commands.add_parser("teardown")
    parser1.add_argument('-s', dest='stackname', type=str, default='kafka', help='stack name')
    parser1.set_defaults(func=teardown_cli)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
