import argparse
import boto3
import json
import os
import subprocess
import sys
import time

from .module.ec2 import EC2Instance
from .module.stack import Stack, check_stack_not_exists, wait_for_stack_creation, teardown_stack, S3_BUCKET
from .module.template import TEMPLATE_DIR

_S3_CF_PATH='cloudformation/zookeeper'
_EC2_PEM = '~/aws.pem'

def main():
    args = parse_args()
    args.func(args)

def template(args):
    stack = template_internal(args.nodes)
    if args.upload:
        stack.validate()
        stack.upload(_S3_CF_PATH)
    else:
        print stack.json()
        stack.validate()

def template_internal(nodes):
    stack = Stack("AWS CloudFormation template for Zookeeper Quorum.")
    for n in xrange(nodes):
        name = instance_name(n)
        subnet = "SubnetId%d" % ( (n % 2) + 1 )
        ec2 = EC2Instance(name, subnet_ref=subnet) \
            .metadata(ec2_metadata(n)) \
            .add_tag("lattice-engines.cluster.type", "Zookeeper")
        stack.add_ec2(ec2)
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
                'ParameterValue': 'sg-61fd941a'
            },
            {
                'ParameterKey': 'InstanceType',
                'ParameterValue': 't2.micro'
            }
        ],
        TimeoutInMinutes=60,
        ResourceTypes=[
            'AWS::*',
        ],
        OnFailure='DELETE',
        Tags=[
            {
                'Key': 'com.lattice-engines.cluster.name',
                'Value': args.stackname
            },
            {
                'Key': 'com.lattice-engines.cluster.type',
                'Value': 'zookeeper'
            },
        ]
    )
    print 'Got StackId: %s' % response['StackId']
    wait_for_stack_creation(client, args.stackname)

def bootstrap(args):
    ips = get_ips(args.stackname)
    print 'Found ips in output:\n', ips
    update_zoo_cfg(ips)

def info(args):
    ips = get_ips(args.stackname)
    print_zk_hosts(ips)

def get_ips(stackname):
    stack = boto3.resource('cloudformation').Stack(stackname)
    ips = {}
    for output in stack.outputs:
        key = output['OutputKey']
        if 'EC2Instance' in key:
            node_id = key.replace('EC2Instance', '') \
                .replace('PrivateIp', '') \
                .replace('PublicIp', '') \
                .replace('URL', '')
            if node_id not in ips:
                ips[node_id] = {}
            if 'PrivateIp' in key:
                ips[node_id]['PrivateIp'] = output['OutputValue']
            elif 'URL' in key:
                ips[node_id]['URL'] = output['OutputValue']
            elif 'PublicIp' in key:
                ips[node_id]['PublicIp'] = output['OutputValue']
    return ips

def update_zoo_cfg(ips):
    private_ips = [None] * len(ips)
    for node_id, node_ips in ips.items():
        private_ips[int(node_id) - 1] = node_ips['PrivateIp']

    zoo_cfg = zk_properties(len(ips), private_ips)
    temp_file = '/tmp/zoo.cfg'
    with open(temp_file, 'w') as tf:
        tf.write('\n'.join(zoo_cfg) + "\n")

    with open(temp_file, 'r') as tf:
        print '\nzoo.cfg content is:\n\n', tf.read()

    public_zk_hosts=[]
    private_zk_hosts=[]
    for node_id, node_ips in ips.items():
        url = 'ec2-user@%s' % node_ips['URL']
        remote_path = '/opt/zookeeper-3.4.6/conf/zoo.cfg'

        print 'Bootstrapping node %s [%s] ...' %(node_id, url)
        t1 = time.time()
        subprocess.call("scp -oStrictHostKeyChecking=no -i %s %s %s:%s" % (_EC2_PEM, temp_file, url, remote_path), shell=True)
        ssh = subprocess.Popen(["ssh", "-oStrictHostKeyChecking=no", "-i", _EC2_PEM, url, "sudo /opt/zookeeper-3.4.6/bin/zkServer.sh start"],
                               shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        result = ssh.stdout.readlines()
        if result == []:
            error = ssh.stderr.readlines()
            print >>sys.stderr, "ERROR: %s" % error
        else:
            print result
        print 'Done. %.2f seconds.' % (time.time() -t1)

        public_zk_hosts.append(node_ips['PublicIp'] + ":2181")
        private_zk_hosts.append(node_ips['PrivateIp'] + ":2181")

    print '\n================================================================================'
    print 'Public ZK Connection String: %s' % ','.join(public_zk_hosts)
    print 'Private ZK Connection String: %s' % ','.join(private_zk_hosts)
    print '================================================================================\n'


def print_zk_hosts(ips):
    public_zk_hosts=[]
    private_zk_hosts=[]
    for node_id, node_ips in ips.items():
        public_zk_hosts.append(node_ips['PublicIp'] + ":2181")
        private_zk_hosts.append(node_ips['PrivateIp'] + ":2181")
    print '\n================================================================================'
    print 'Public ZK Connection String: %s' % ','.join(public_zk_hosts)
    print 'Private ZK Connection String: %s' % ','.join(private_zk_hosts)
    print '================================================================================\n'

def teardown(args):
    client = boto3.client('cloudformation')
    teardown_stack(client, args.stackname)

def instance_name(idx):
    return "EC2Instance%d" % (idx + 1)

def ec2_metadata(idx):
    json_file = os.path.join(TEMPLATE_DIR, 'zookeeper', 'ec2_metadata.json')
    with open(json_file) as f:
        metadata = json.load(f)
        files_node = metadata["AWS::CloudFormation::Init"]["prepare"]["files"]
        files_node["/var/lib/zookeeper/myid"]["content"] = "%d" % (idx + 1)
    return metadata

def zk_properties(nodes, ips):
    lines = [
        "dataDir=/var/lib/zookeeper",
        "clientPort=2181",
        "tickTime=2000",
        "initLimit=10",
        "syncLimit=5",
        "maxClientCnxns=256"
    ]
    for i in xrange(nodes):
        lines.append("server.%d=%s:2888:3888" % (i + 1, ips[i]))
    return lines

def parse_args():
    parser = argparse.ArgumentParser(description='Zookeeper CloudFormation management')
    commands = parser.add_subparsers(help="commands")

    parser1 = commands.add_parser("template")
    parser1.add_argument('-n', dest='nodes', type=int, default=3, help='number of nodes')
    parser1.add_argument('-u', dest='upload', action='store_true', help='upload to S3')
    parser1.set_defaults(func=template)

    parser1 = commands.add_parser("provision")
    parser1.add_argument('-s', dest='stackname', type=str, default='zookeeper', help='stack name')
    parser1.set_defaults(func=provision)

    parser1 = commands.add_parser("bootstrap")
    parser1.add_argument('-s', dest='stackname', type=str, default='zookeeper', help='stack name')
    parser1.set_defaults(func=bootstrap)

    parser1 = commands.add_parser("info")
    parser1.add_argument('-s', dest='stackname', type=str, default='zookeeper', help='stack name')
    parser1.set_defaults(func=info)

    parser1 = commands.add_parser("teardown")
    parser1.add_argument('-s', dest='stackname', type=str, default='zookeeper', help='stack name')
    parser1.set_defaults(func=teardown)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
