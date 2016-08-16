import argparse
import boto3
import httplib
import json
import os
import subprocess
import sys
import time

from .module.ec2 import EC2Instance
from .module.parameter import PARAM_INSTANCE_TYPE, PARAM_SECURITY_GROUP, PARAM_VPC_ID, PARAM_SUBNET_1, PARAM_SUBNET_2, PARAM_SUBNET_3, PARAM_KEY_NAME, PARAM_ENVIRONMENT
from .module.stack import Stack, check_stack_not_exists, wait_for_stack_creation, teardown_stack
from .module.template import TEMPLATE_DIR
from ..conf import AwsEnvironment

_S3_CF_PATH='cloudformation/zookeeper'

SUBNETS = [PARAM_SUBNET_1, PARAM_SUBNET_2, PARAM_SUBNET_3]

def main():
    args = parse_args()
    args.func(args)

def template_cli(args):
    template(args.environment, args.nodes, upload=args.upload, observers=args.observers)

def template(environment, nodes, upload=False, observers=False):
    stack = Stack("AWS CloudFormation template for Zookeeper Quorum.")
    stack.add_params([PARAM_INSTANCE_TYPE, PARAM_SECURITY_GROUP])
    config = AwsEnvironment(environment)

    if observers:
        nodes = len(config.zk_observer_ips())

    for n in xrange(nodes):
        name = "EC2Instance%d" % (n + 1)
        subnet = SUBNETS[n % 3]
        ec2 = EC2Instance(name, PARAM_INSTANCE_TYPE, PARAM_KEY_NAME) \
            .set_metadata(ec2_metadata(n)) \
            .mount("/dev/xvdb", 10) \
            .add_sg(PARAM_SECURITY_GROUP) \
            .set_subnet(subnet)

        if observers:
            ip = config.zk_observer_ips()[n % 3]
            ec2.set_private_ip(ip)

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
            PARAM_VPC_ID.config(config.vpc()),
            PARAM_SUBNET_1.config(config.private_subnet_1()),
            PARAM_SUBNET_2.config(config.private_subnet_2()),
            PARAM_SUBNET_3.config(config.private_subnet_3()),
            PARAM_SECURITY_GROUP.config(config.zk_sg()),
            PARAM_INSTANCE_TYPE.config("t2.medium"),
            PARAM_KEY_NAME.config(config.ec2_key()),
            PARAM_ENVIRONMENT.config(environment)
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
    bootstrap(args.stackname, args.keyfile, args.consul)

def bootstrap(stackname, pem, consul):
    ips = get_ips(stackname)
    print 'Found ips in output:\n', ips
    zk = update_zoo_cfg(pem, ips)
    if consul is not None:
        print 'Saving zk connection strings to consul server %s' % consul
        write_to_consul(consul, "%s/zk" % stackname, zk + "/" + stackname)
    return zk

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
                .replace('Ip', '') \
                .replace('Url', '')
            if node_id not in ips:
                ips[node_id] = {}
            if 'Ip' in key:
                ips[node_id]['Ip'] = output['OutputValue']
            elif 'Url' in key:
                ips[node_id]['Url'] = output['OutputValue']
    return ips

def update_zoo_cfg(pem, ips):
    private_ips = [None] * len(ips)
    for node_id, node_ips in ips.items():
        private_ips[int(node_id) - 1] = node_ips['Ip']

    zoo_cfg = zk_properties(len(ips), private_ips)
    temp_file = '/tmp/zoo.cfg'
    with open(temp_file, 'w') as tf:
        tf.write('\n'.join(zoo_cfg) + "\n")

    with open(temp_file, 'r') as tf:
        print '\nzoo.cfg content is:\n\n', tf.read()

    zk_hosts=[]
    for node_id, node_ips in ips.items():
        url = 'ec2-user@%s' % node_ips['Ip']
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

        zk_hosts.append(node_ips['Ip'] + ":2181")
        sys.stdout.flush()

    print '\n================================================================================'
    print 'ZK Connection String: %s' % ','.join(zk_hosts)
    print '================================================================================\n'
    return ','.join(zk_hosts)

def print_zk_hosts(ips):
    zk_hosts=[]
    for node_id, node_ips in ips.items():
        zk_hosts.append(node_ips['Ip'] + ":2181")
    print '\n================================================================================'
    print 'ZK Connection String: %s' % ','.join(zk_hosts)
    print '================================================================================\n'
    return  ','.join(zk_hosts)

def teardown_cli(args):
    teardown(args.stackname, consul=args.consul)

def teardown(stackname, consul=None):
    client = boto3.client('cloudformation')
    teardown_stack(client, stackname)
    if consul is not None:
        remove_from_consul(consul, "%s" % stackname)

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

def write_to_consul(server, key, value):
    conn = httplib.HTTPConnection(server)
    conn.request("PUT", "/v1/kv/%s" % key, value)
    response = conn.getresponse()
    print response.status, response.reason

def remove_from_consul(server, key):
    conn = httplib.HTTPConnection(server)
    conn.request("DELETE", "/v1/kv/%s?recurse" % key)
    response = conn.getresponse()
    print response.status, response.reason

def parse_args():
    parser = argparse.ArgumentParser(description='Zookeeper CloudFormation management')
    commands = parser.add_subparsers(help="commands")

    parser1 = commands.add_parser("template")
    parser1.add_argument('-e', dest='environment', type=str, default='qacluster', choices=['qacluster','prodcluster'], help='environment')
    parser1.add_argument('-n', dest='nodes', type=int, default=3, help='number of nodes')
    parser1.add_argument('-u', dest='upload', action='store_true', help='upload to S3')
    parser1.add_argument('--observers', dest='observers', action='store_true', help='fixed ip observers attached to BPDC zk')
    parser1.set_defaults(func=template_cli)

    parser1 = commands.add_parser("provision")
    parser1.add_argument('-e', dest='environment', type=str, default='qacluster', choices=['qacluster','prodcluster'], help='environment')
    parser1.add_argument('-s', dest='stackname', type=str, default='zookeeper', help='stack name')
    parser1.set_defaults(func=provision_cli)

    parser1 = commands.add_parser("bootstrap")
    parser1.add_argument('-e', dest='environment', type=str, default='qacluster', choices=['qacluster','prodcluster'], help='environment')
    parser1.add_argument('-k', dest='keyfile', type=str, default='~/aws.pem', help='the pem key file used to ssh ec2')
    parser1.add_argument('-s', dest='stackname', type=str, default='zookeeper', help='stack name')
    parser1.add_argument('-c', dest='consul', type=str, help='consul server address')
    parser1.add_argument('--observers', dest='observers', action='store_true', help='fixed ip observers attached to BPDC zk')
    parser1.set_defaults(func=bootstrap_cli)

    parser1 = commands.add_parser("info")
    parser1.add_argument('-e', dest='environment', type=str, default='qacluster', choices=['qacluster','prodcluster'], help='environment')
    parser1.add_argument('-s', dest='stackname', type=str, default='zookeeper', help='stack name')
    parser1.set_defaults(func=info)

    parser1 = commands.add_parser("teardown")
    parser1.add_argument('-s', dest='stackname', type=str, default='zookeeper', help='stack name')
    parser1.add_argument('-c', dest='consul', type=str, help='consul server address')
    parser1.set_defaults(func=teardown_cli)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
