import argparse
import boto3
import json
import math
import os

from .kafka_profile import KafkaProfile, DEFAULT_PROFILE
from .module.autoscaling import AutoScalingGroup, LaunchConfiguration
from .module.ec2 import _ec2_params, EC2Instance
from .module.ecs import ECSCluster, ECSService, TaskDefinition, ContainerDefinition, Volume
from .module.elb import ElasticLoadBalancer
from .module.stack import Stack, teardown_stack, check_stack_not_exists, wait_for_stack_creation
from .module.template import TEMPLATE_DIR
from ..conf import AwsEnvironment

_S3_CF_PATH='cloudformation/kafka'
_EC2_PEM = '~/aws.pem'

_LOG_SIZE=10

def main():
    args = parse_args()
    args.func(args)

def template_cli(args):
    template(args.environment, args.upload)
    calc_heap_log(args.pth, args.ath)

def template(environment, upload):
    stack = create_template()
    if upload:
        stack.validate()
        stack.upload(environment, _S3_CF_PATH)
    else:
        print stack.json()
        stack.validate()

def create_template():
    stack = Stack("AWS CloudFormation template for Kafka ECS container instances.")
    stack.add_params(extra_param()).add_params(_ec2_params())

    #ec2role = ECSServiceRole("EC2Role")
    #instanceprofile = InstanceProfile("EC2InstanceProfile", ec2role)
    #stack.add_resources([ec2role, instanceprofile])

    # Broker resources
    elb9092 = ElasticLoadBalancer("lb9092").listen("9092")
    ecscluster = ECSCluster("BrokerCluster")
    asgroup, launchconfig = create_bkr_asgroup(ecscluster, [ elb9092 ])
    # ecsrole = ECSServiceRole("ECSServiceRole")
    bkr, bkr_task = broker_service(ecscluster, asgroup)
    stack.add_resources([elb9092, ecscluster, asgroup, launchconfig, bkr, bkr_task])

    sr_rsrcs = sr_resources(bkr, elb9092)
    elb9022, elb9024 = sr_rsrcs[:2]
    stack.add_resources(sr_rsrcs)

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
    return provision(args.environment, args.stackname, args.zkhosts, profile=args.profile)

def provision(environment, stackname, zkhosts, profile=None):
    if profile is None:
        profile = DEFAULT_PROFILE
    else:
        profile = KafkaProfile(profile)

    config = AwsEnvironment(environment)

    client = boto3.client('cloudformation')
    check_stack_not_exists(client, stackname)
    response = client.create_stack(
        StackName=stackname,
        TemplateURL='https://s3.amazonaws.com/%s' % os.path.join(config.cf_bucket(), _S3_CF_PATH, 'template.json'),
        Parameters=[
            {
                'ParameterKey': 'VpcId',
                'ParameterValue': config.vpc()
            },
            {
                'ParameterKey': 'SubnetId1',
                'ParameterValue': config.public_subnet_1()
            },
            {
                'ParameterKey': 'SubnetId2',
                'ParameterValue': config.public_subnet_2()
            },
            {
                'ParameterKey': 'KeyName',
                'ParameterValue': config.ec2_key()
            },
            {
                'ParameterKey': 'SecurityGroupId',
                'ParameterValue': config.kafka_sg()
            },
            {
                'ParameterKey': 'BrokerInstanceType',
                'ParameterValue': profile.instance_type()
            },
            {
                'ParameterKey': 'SRInstanceType',
                'ParameterValue': profile.sr_instance_type()
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
                'ParameterKey': 'BrokerHeapSize',
                'ParameterValue': profile.broker_heap()
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


def consul_master_service(ecscluster, asgroup):
    container = ContainerDefinition("consul", "progrium/consul") \
        .mem_mb(1000).publish_port(8500, 8500) \
        .command(["-server", "-bootstrap", "-bootstrap-expect", "4", "-ui-dir", "/ui"])

    task = TaskDefinition("ConsulMasterTask")
    task.add_container(container)

    service = ECSService("ConsulMaster", ecscluster, task, 1).set_min_max_percent(100, 200).depends_on(asgroup)
    return service, task

def consul_follower_service(ecscluster, elb8500, master):
    assert isinstance(elb8500, ElasticLoadBalancer)

    container = ContainerDefinition("consul", "progrium/consul") \
        .mem_mb(1000).publish_port(8500, 8500) \
        .command(["-server", "-join", { "Fn::GetAtt": [ elb8500.logical_id(), "DNSName" ] } ])

    task = TaskDefinition("ConsulFollowerTask")
    task.add_container(container)

    service = ECSService("ConsulFollower", ecscluster, task, 1).set_min_max_percent(100, 200)\
        .depends_on(elb8500).depends_on(master)
    return service, task

def create_bkr_asgroup(ecscluster, elbs):
    asgroup = AutoScalingGroup("BrokerScalingGroup")
    launchconfig = LaunchConfiguration("BrokerContainerPool", instance_type_ref="BrokerInstanceType")
    launchconfig.set_metadata(bkr_metadata(launchconfig, ecscluster))
    launchconfig.set_userdata(userdata(launchconfig, asgroup))
    launchconfig.mount("/dev/xvda", 8)

    disks = broker_log_devices(_LOG_SIZE)
    for disk in disks:
        launchconfig.mount("/dev/xvd%s" % disk['label'], disk['size'])

    asgroup.add_pool(launchconfig)
    asgroup.attach_elbs(elbs)
    return asgroup, launchconfig

def broker_service(ecscluster, asgroup):
    intaddr = Volume("internaladdr", "/etc/internaladdr.txt")
    extaddr= Volume("externaladdr", "/etc/externaladdr.txt")

    disks = broker_log_devices(_LOG_SIZE)
    log_vols = [Volume("log%s" % d['label'], "/mnt/kafka/log/%s" % d['label']) for d in disks]

    container = ContainerDefinition("bkr", { "Fn::Join" : [ "", [
        { "Fn::FindInMap" : [ "Environment2Props", {"Ref" : "Environment"}, "EcrRegistry" ] },
        "/kafka"
        ] ]}) \
        .mem_mb({ "Ref" : "BrokerMemory" }).publish_port(9092, 9092) \
        .set_env("ZK_HOSTS", { "Ref" : "ZookeeperHosts" }) \
        .set_env("LOG_DIRS", ",".join("/mnt/kafka/log/%s" % d['label'] for d in disks)) \
        .set_env("RACK_ID", { "Ref" : "AWS::Region" }) \
        .set_env("KAFKA_HEAP_OPTS", {"Fn::Join" : [ "", [
            "-Xmx", { "Ref": "BrokerHeapSize" }, " -Xms", { "Ref": "BrokerHeapSize" }
        ] ]}) \
        .set_logging({
            "LogDriver": "awslogs",
            "Options": {
                "awslogs-group": "docker-kafka-broker",
                "awslogs-region": { "Ref": "AWS::Region" }
            }
        }) \
        .mount("/etc/internaladdr.txt", intaddr) \
        .mount("/etc/externaladdr.txt", extaddr)

    for v, d in zip(log_vols, disks):
        container.mount("/mnt/kafka/log/%s" % d['label'], v)

    task = TaskDefinition("BrokerTask")
    task.add_container(container)\
        .add_volume(intaddr)\
        .add_volume(extaddr)

    for v in log_vols:
        task.add_volume(v)

    service = ECSService("Broker", ecscluster, task, { "Ref": "Brokers" }).set_min_max_percent(50, 200)\
        .depends_on(asgroup)
    return service, task

def sr_resources(bkr_service, elb9092):
    elb9022 = ElasticLoadBalancer("lb9022").listen("9022", "80", protocol="http")
    # elb9023 = ElasticLoadBalancer("lb9023").listen("9023", "80", protocol="http")
    elb9024 = ElasticLoadBalancer("lb9024").listen("9024", "80", protocol="http")
    ecscluster = ECSCluster("SchemaRegistryCluster")
    asgroup, launchconfig = create_sr_asgroup(ecscluster, [elb9022, elb9024])
    sr, sr_task = schema_registry_service(ecscluster, bkr_service)
    conn, conn_task = kafka_connect_service(ecscluster, elb9092, elb9022, sr)
    # kr, kr_task = kafka_rest_service(ecscluster, elb9022, sr)
    return elb9022, elb9024, ecscluster, asgroup, launchconfig, sr, sr_task, conn, conn_task

def create_sr_asgroup(ecscluster, elbs):
    asgroup = AutoScalingGroup("SchemaRegistryScalingGroup").set_capacity(2).set_max_size(4)
    launchconfig = LaunchConfiguration("SchemaRegistryContainerPool", instance_type_ref="SRInstanceType")
    launchconfig.set_metadata(sr_metadata(launchconfig, ecscluster))
    launchconfig.set_userdata(userdata(launchconfig, asgroup))
    asgroup.add_pool(launchconfig)
    asgroup.attach_elbs(elbs)
    return asgroup, launchconfig

def schema_registry_service(ecscluster, broker):
    intaddr = Volume("internaladdr", "/etc/internaladdr.txt")
    extaddr= Volume("externaladdr", "/etc/externaladdr.txt")
    container = ContainerDefinition("sr", { "Fn::Join" : [ "", [
            { "Fn::FindInMap" : [ "Environment2Props", {"Ref" : "Environment"}, "EcrRegistry" ] },
            "/schema-registry"
        ] ]}) \
        .mem_mb(1800).publish_port(9022, 9022) \
        .set_env("SCHEMA_REGISTRY_HEAP_OPTS", "-Xms1400m -Xms1400m") \
        .set_env("ZK_HOSTS", { "Ref" : "ZookeeperHosts" }) \
        .set_env("ZK_NAMESPACE", {"Fn::Join" : ["", [ { "Ref" : "AWS::StackName" }, "_schema_registry" ]]}) \
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

    service = ECSService("SchemaRegistry", ecscluster, task, 2).set_min_max_percent(50, 200)\
        .depends_on(broker)
    return service, task

def kafka_rest_service(ecscluster, elb9022, sr):
    container = ContainerDefinition("kr", { "Fn::Join" : [ "", [
            { "Fn::FindInMap" : [ "Environment2Props", {"Ref" : "Environment"}, "EcrRegistry" ] },
            "/kafka-rest"
        ] ]}) \
        .mem_mb(600).publish_port(9023, 9023) \
        .set_env("KAFKAREST_HEAP_OPTS", "-Xms512m -Xms512m") \
        .set_env("ZK_HOSTS", { "Ref" : "ZookeeperHosts" }) \
        .set_env("SR_ADDRESS",
                 {"Fn::Join" : [ "", [
                     "http://", {"Fn::GetAtt": [ elb9022.logical_id(), "DNSName" ] }
                 ]]})

    task = TaskDefinition("KafkaRESTTask")
    task.add_container(container)

    service = ECSService("KafkaREST", ecscluster, task, 2).set_min_max_percent(50, 200).depends_on(sr)
    return service, task

def kafka_connect_service(ecscluster, elb9092, elb9022, sr):
    container = ContainerDefinition("connect", { "Fn::Join" : [ "", [
            { "Fn::FindInMap" : [ "Environment2Props", {"Ref" : "Environment"}, "EcrRegistry" ] },
            "/kafka-connect"
        ] ]}) \
        .mem_mb(1800).publish_port(9024, 9024) \
        .set_env("KAFKA_HEAP_OPTS", "-Xms1400m -Xms1400m") \
        .set_env("BOOTSTRAP_SERVERS", { "Fn::Join" : [ "", [
            { "Fn::GetAtt": [ elb9092.logical_id(), "DNSName" ] } , ":9092"] ]}) \
        .set_env("SR_ADDRESS", { "Fn::Join" : [ "", [
            "http://", { "Fn::GetAtt": [ elb9022.logical_id(), "DNSName" ] }] ]}) \
        .set_env("GROUP_ID", { "Fn::Join" : [ "", [ "cf-", { "Ref": "AWS::StackName" } ] ]}) \
        .set_logging({
            "LogDriver": "awslogs",
            "Options": {
                "awslogs-group": "docker-kafka-connect",
                "awslogs-region": { "Ref": "AWS::Region" }
            }
        })

    task = TaskDefinition("ConnectWorkerTask")
    task.add_container(container)

    service = ECSService("ConnectWorker", ecscluster, task, 2).set_min_max_percent(50, 200).depends_on(sr)
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

def extra_param():
    json_file = os.path.join(TEMPLATE_DIR, 'kafka', 'params.json')
    with open(json_file) as f:
        text = f.read()
        return json.loads(text)


def bkr_metadata(ec2, ecscluster):
    assert isinstance(ec2, EC2Instance) or isinstance(ec2, LaunchConfiguration)
    assert isinstance(ecscluster, ECSCluster)
    metadata = {
        "AWS::CloudFormation::Init" : {
            "configSets": {
                "bootstrap": [
                    "prepare",
                    "install"
                ],
                "reload": [ "install" ]
            },
            "prepare" : {
                "packages" : {
                    "yum" : {
                        "xfsprogs" : []
                    }
                },
                "files" : {
                    "/tmp/mount_volume.sh": {
                        "content": {
                            "Fn::Join": [
                                "\n",
                                [ "#!/usr/bin/env bash",
                                  "mountebs() {",
                                  "  mkdir -p $2",
                                  "  file -s $1",
                                  "  mkfs -t xfs $1",
                                  "  echo \"${1}       ${2}   xfs    defaults,nofail        0       2\" >> /etc/fstab",
                                  "  RETVAL=$?",
                                  "  return $RETVAL"
                                  "}"
                                  ]
                            ]
                        },
                        "mode": "000777",
                        "owner": "root",
                        "group": "root"
                    }
                },
                "commands" : {
                    "01_mount" : {
                        "command" : { "Fn::Join": [ "\n", [
                            "bash /mount_volumn.sh",
                            "mount -a"
                        ] ] }
                    }
                }
            },
            "install" : {
                "files" : {
                    "/etc/cfn/cfn-hup.conf" : {
                        "content" : { "Fn::Join" : ["", [
                            "[main]\n",
                            "stack=", { "Ref" : "AWS::StackId" }, "\n",
                            "region=", { "Ref" : "AWS::Region" }, "\n"
                        ]]},
                        "mode"    : "000400",
                        "owner"   : "root",
                        "group"   : "root"
                    },
                    "/etc/cfn/hooks.d/cfn-auto-reloader.conf" : {
                        "content": { "Fn::Join" : ["", [
                            "[cfn-auto-reloader-hook]\n",
                            "triggers=post.update\n",
                            "path=Resources.ContainerInstances.Metadata.AWS::CloudFormation::Init\n",
                            "action=/opt/aws/bin/cfn-init -v",
                            "         -c reload",
                            "         --stack ", { "Ref" : "AWS::StackName" },
                            "         --resource %s " % ec2.logical_id(),
                            "         --region ", { "Ref" : "AWS::Region" }, "\n",
                            "runas=root\n"
                        ]]}
                    }
                },
                "commands" : {
                    "01_save_ip" : {
                        "command" : { "Fn::Join": [ "\n", [
                            "#!/bin/bash",
                            "ADDR=`ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'`",
                            "echo $ADDR >> /etc/internaladdr.txt",
                            "PUBADDR=`curl http://169.254.169.254/latest/meta-data/public-ipv4`",
                            "echo $PUBADDR >> /etc/externaladdr.txt"
                        ] ] }
                    },
                    "02_add_instance_to_cluster" : {
                        "command" : { "Fn::Join": [ "", [
                            "#!/bin/bash\n",
                            "rm -rf /etc/ecs/ecs.config\n",
                            "touch /etc/ecs/ecs.config\n",
                            "echo ECS_CLUSTER=", ecscluster.ref(), " >> /etc/ecs/ecs.config\n",
                            "echo ECS_AVAILABLE_LOGGING_DRIVERS=[\\\"json-file\\\", \\\"awslogs\\\"] >> /etc/ecs/ecs.config\n",
                            "echo ECS_RESERVED_PORTS=[22, 5000] >> /etc/ecs/ecs.config\n"
                        ] ] }
                    }
                },
                "services" : {
                    "sysvinit" : {
                        "cfn-hup" : { "enabled" : "true", "ensureRunning" : "true", "files" : ["/etc/cfn/cfn-hup.conf", "/etc/cfn/hooks.d/cfn-auto-reloader.conf"] }
                    }
                }
            }
        }
    }

    disks = broker_log_devices(_LOG_SIZE)
    for d in disks:
        metadata["AWS::CloudFormation::Init"]["prepare"]["files"]["/tmp/mount_volume.sh"]["content"]["Fn::Join"][1]\
            .append("mountebs /dev/xvd%s /mnt/kafka/log/%s" % (d['label'], d['label']))

    return metadata


def userdata(ec2, asgroup):
    assert isinstance(ec2, EC2Instance) or isinstance(ec2, LaunchConfiguration)
    assert isinstance(asgroup, AutoScalingGroup)
    return { "Fn::Base64" : { "Fn::Join" : ["", [
        "#!/bin/bash -xe\n",
        "yum install -y aws-cfn-bootstrap\n",

        "/opt/aws/bin/cfn-init -v",
        "         -c bootstrap"
        "         --stack ", { "Ref" : "AWS::StackName" },
        "         --resource %s " % ec2.logical_id(),
        "         --region ", { "Ref" : "AWS::Region" }, "\n",

        "/opt/aws/bin/cfn-signal -e $? ",
        "         --stack ", { "Ref" : "AWS::StackName" },
        "         --resource %s " % asgroup.logical_id(),
        "         --region ", { "Ref" : "AWS::Region" }, "\n"
    ]]}}

def sr_metadata(ec2, ecscluster):
    config = bkr_metadata(ec2, ecscluster)["AWS::CloudFormation::Init"]["install"]
    return {
        "AWS::CloudFormation::Init" : {
            "configSets": {
                "bootstrap": [ "install" ],
                "reload": [ "install" ]
            },
            "install": config
        }
    }

def calc_heap_log(peak, avg):
    heap = peak * 1024 * 30
    log_size = avg * 2
    print 'for expected peak throughput %.2f GB/sec, average throughput %.2f GB/day' % (peak, avg)
    print 'total heap size recommended: %d MB' % int(heap)
    print 'total log size recommended: %d GB' % int(log_size)

def broker_log_devices(log_size):
    disks = []
    labels = 'bcdefghijk'
    if log_size <= 440:
        disk_size = max(int(math.floor(log_size / 11.0)), 10)
        num_disks, rem = max(int(math.floor(1.0 * log_size / disk_size)), 1), log_size % disk_size
        for i in xrange(num_disks):
            disks.append({
                'label': labels[i],
                'size': disk_size + rem if i == (num_disks - 1) else disk_size,
                'type': 'gp2'
            })
    else:
        disks.append({
            'label': 'b',
            'size': max(log_size, 500),
            'type': 'st1'
        })
    print ('According to log size of %d GB, going to mount disks [ ' % log_size) + ','.join(str(d['size']) for d in disks) + ' ] GB'
    return disks

def parse_args():
    parser = argparse.ArgumentParser(description='Kafka ECS CloudFormation management')
    commands = parser.add_subparsers(help="commands")

    parser1 = commands.add_parser("template")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qa','prod'], help='environment')
    parser1.add_argument('-u', dest='upload', action='store_true', help='upload to S3')
    parser1.add_argument('--peak-throughput', dest='pth', type=float, default='0.5', help='expected peak throughput in GB/sec')
    parser1.add_argument('--avg-throughput', dest='ath', type=float, default='2', help='expected average throughput in GB/day')
    parser1.set_defaults(func=template_cli)

    parser1 = commands.add_parser("provision")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qa','prod'], help='environment')
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
