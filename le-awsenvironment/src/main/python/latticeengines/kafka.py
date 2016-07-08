import argparse

from cf import zookeeper
from cf.kafka import manage as kafka


def main():
    args = parse_args()
    args.func(args)

def provision_cli(args):
    provision(args.environment, args.stackname, args.profile, args.keyfile, consul=args.consul)

def provision(environment, stackname, profile, keyfile, consul=None):
    # update cloud formation templates
    zookeeper.template(environment, 4, upload=True)
    kafka.template(environment, upload=True)

    # provision and bootstrap zookeeper
    zookeeper.provision(environment, stackname + "-zk")
    pub_zk_hosts, pri_zk_hosts = zookeeper.bootstrap(stackname + "-zk", keyfile, consul)

    # provision kafka cloud formation
    elbs = kafka.provision(environment, stackname, pri_zk_hosts, profile)

    print "public zk address: %s/%s" % (pub_zk_hosts, stackname)
    print "private zk address: %s/%s" % (pri_zk_hosts, stackname)
    print "broker address: %s:9092" % elbs["BrokerLoadBalancer"]
    print "schema registry address: http://%s/" % elbs["SchemaRegistryLoadBalancer"]
    print "kafka connect address: http://%s/" % elbs["KafkaConnectLoadBalancer"]

    if consul is not None:
        print "You can also find these addresses on consul server: http://%s:8500/ui/#/dc1/kv/" % consul

def teardown_cli(args):
    teardown(args.stackname, consul=args.consul)

def teardown(stackname, consul=None):
    kafka.teardown(stackname)
    zookeeper.teardown(stackname+"-zk", consul)

def describe(args):
    pass

def parse_args():
    parser = argparse.ArgumentParser(description='Kafka full stack provision/teardown tool')
    commands = parser.add_subparsers(help="commands")

    parser1 = commands.add_parser("provision")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qa','prod'], help='environment')
    parser1.add_argument('-s', dest='stackname', type=str, default='kafka', help='stack name')
    parser1.add_argument('-p', dest='profile', type=str, help='profile file')
    parser1.add_argument('-k', dest='keyfile', type=str, default='~/aws.pem', help='the pem key file used to ssh ec2')
    parser1.add_argument('-c', dest='consul', type=str, help='consul server address')
    parser1.set_defaults(func=provision_cli)

    parser1 = commands.add_parser("describe")
    parser1.add_argument('-s', dest='stackname', type=str, default='kafka', help='stack name')
    parser1.set_defaults(func=describe)

    parser1 = commands.add_parser("teardown")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qa','prod'], help='environment')
    parser1.add_argument('-s', dest='stackname', type=str, default='kafka', help='stack name')
    parser1.add_argument('-c', dest='consul', type=str, help='consul server address')
    parser1.set_defaults(func=teardown_cli)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()