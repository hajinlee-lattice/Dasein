import argparse
import boto3
import json
import os
import subprocess
import sys
import time

from .module.ec2 import EC2Instance
from .module.stack import Stack, check_stack_not_exists, wait_for_stack_creation, teardown_stack
from .module.template import TEMPLATE_DIR
from ..conf import AwsEnvironment

_S3_CF_PATH='cloudformation/zookeeper'

def main():
    args = parse_args()
    args.func(args)

def template_cli(args):
    template(args.environment, args.nodes, upload=args.upload)

def template(environment, nodes, upload=False):
    stack = Stack("AWS CloudFormation template for Zookeeper Quorum.")
    for n in xrange(nodes):
        name = "EC2Instance%d" % (n + 1)
        subnet = "SubnetId%d" % ( (n % 2) + 1 )
        ec2 = EC2Instance(name, subnet_ref=subnet) \
            .set_metadata(ec2_metadata(n)) \
            .mount("/dev/xvdb", 10) \
            .add_tag("lattice-engines.cluster.type", "Zookeeper")
        stack.add_resource(ec2)
    if upload:
        stack.validate()
        stack.upload(environment, _S3_CF_PATH)
    else:
        print stack.json()
        stack.validate()

def provision_cli(args):
    provision(args.environment, args.stackname)

def provision(environment, stackname):
    config = AwsEnvironment(environment)
    client = boto3.client('cloudformation')
    check_stack_not_exists(client, stackname)
    bucket = AwsEnvironment(environment).cf_bucket()
    response = client.create_stack(
        StackName=stackname,
        TemplateURL='https://s3.amazonaws.com/%s' % os.path.join(bucket, _S3_CF_PATH, 'template.json'),
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
                'ParameterValue': config.zk_sg()
            },
            {
                'ParameterKey': 'InstanceType',
                'ParameterValue': 't2.medium'
            }
        ],
        TimeoutInMinutes=60,
        ResourceTypes=[
            'AWS::*',
        ],
        OnFailure='ROLLBACK',
        Tags=[
            {
                'Key': 'com.lattice-engines.cluster.name',
                'Value': stackname
            },
            {
                'Key': 'com.lattice-engines.cluster.type',
                'Value': 'zookeeper'
            },
        ]
    )
    print 'Got StackId: %s' % response['StackId']
    sys.stdout.flush()
    wait_for_stack_creation(client, stackname)

def bootstrap_cli(args):
    bootstrap(args.stackname, args.keyfile)

def bootstrap(stackname, pem):
    ips = get_ips(stackname)
    print 'Found ips in output:\n', ips
    return update_zoo_cfg(pem, ips)

def info(args):
    ips = get_ips(args.stackname)
    return print_zk_hosts(ips)

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

def update_zoo_cfg(pem, ips):
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
        remote_path = '/opt/zookeeper/conf/zoo.cfg'

        print 'Bootstrapping node %s [%s] ...' %(node_id, url)
        t1 = time.time()

        if pem == "ssh-agent":
            subprocess.call("scp -oStrictHostKeyChecking=no %s %s:%s" % (temp_file, url, remote_path), shell=True)
            ssh = subprocess.Popen(["ssh", "-oStrictHostKeyChecking=no", url,
                                "sudo service zookeeper restart"],
                               shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        else:
            subprocess.call("scp -oStrictHostKeyChecking=no -i %s %s %s:%s" % (pem, temp_file, url, remote_path), shell=True)
            ssh = subprocess.Popen(["ssh", "-oStrictHostKeyChecking=no", "-i", pem, url,
                                    "sudo service zookeeper restart"],
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
        sys.stdout.flush()

    print '\n================================================================================'
    print 'Public ZK Connection String: %s' % ','.join(public_zk_hosts)
    print 'Private ZK Connection String: %s' % ','.join(private_zk_hosts)
    print '================================================================================\n'
    return ','.join(public_zk_hosts), ','.join(private_zk_hosts)

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
    return  ','.join(public_zk_hosts), ','.join(private_zk_hosts)

def teardown_cli(args):
    teardown(args.stackname)

def teardown(stackname):
    client = boto3.client('cloudformation')
    teardown_stack(client, stackname)

def instance_name(idx):
    return "EC2Instance%d" % (idx + 1)

def ec2_metadata(idx):
    json_file = os.path.join(TEMPLATE_DIR, 'zookeeper', 'ec2_metadata.json')
    with open(json_file, 'r') as f:
        metadata = json.load(f)
        files_node = metadata["AWS::CloudFormation::Init"]["prepare"]["files"]
        files_node["/var/lib/zookeeper/myid"]["content"] = "%d" % (idx + 1)
        files_node["/etc/init.d/zookeeper"]["content"] = init_script()
    return metadata

def init_script():
    init_file = os.path.join(TEMPLATE_DIR, 'zookeeper', 'init.d')
    with open(init_file, 'r') as f:
        return f.read()

def zk_properties(nodes, ips):
    lines = [
        "dataDir=/var/lib/zookeeper",
        "dataLogDir=/mnt/zookeeper/data-log",
        "clientPort=2181",
        "tickTime=2000",
        "initLimit=10",
        "syncLimit=5",
        "maxClientCnxns=256",
        "autopurge.snapRetainCount=5",
        "autopurge.purgeInterval=2"
    ]
    for i in xrange(nodes):
        lines.append("server.%d=%s:2888:3888" % (i + 1, ips[i]))
    return lines

def parse_args():
    parser = argparse.ArgumentParser(description='Zookeeper CloudFormation management')
    commands = parser.add_subparsers(help="commands")

    parser1 = commands.add_parser("template")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qa','prod'], help='environment')
    parser1.add_argument('-n', dest='nodes', type=int, default=3, help='number of nodes')
    parser1.add_argument('-u', dest='upload', action='store_true', help='upload to S3')
    parser1.set_defaults(func=template_cli)

    parser1 = commands.add_parser("provision")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qa','prod'], help='environment')
    parser1.add_argument('-s', dest='stackname', type=str, default='zookeeper', help='stack name')
    parser1.set_defaults(func=provision_cli)

    parser1 = commands.add_parser("bootstrap")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qa','prod'], help='environment')
    parser1.add_argument('-k', dest='keyfile', type=str, default='~/aws.pem', help='the pem key file used to ssh ec2')
    parser1.add_argument('-s', dest='stackname', type=str, default='zookeeper', help='stack name')
    parser1.set_defaults(func=bootstrap_cli)

    parser1 = commands.add_parser("info")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qa','prod'], help='environment')
    parser1.add_argument('-s', dest='stackname', type=str, default='zookeeper', help='stack name')
    parser1.set_defaults(func=info)

    parser1 = commands.add_parser("teardown")
    parser1.add_argument('-s', dest='stackname', type=str, default='zookeeper', help='stack name')
    parser1.set_defaults(func=teardown_cli)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
