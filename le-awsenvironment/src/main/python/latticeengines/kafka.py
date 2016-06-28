import argparse

from cf import zookeeper, kafka


def main():
    args = parse_args()
    args.func(args)


def provision_cli(args):
    provision(args.stackname, args.profile)

def provision(stackname, profile):
    # update cloud formation templates
    zookeeper.template(3, upload=True)
    kafka.template(upload=True)

    # provision and bootstrap zookeeper
    zookeeper.provision(stackname + "-zk")
    pub_zk_hosts, pri_zk_hosts = zookeeper.bootstrap(stackname + "-zk")

    # provision kafka cloud formation
    elbs = kafka.provision(stackname, pri_zk_hosts + "/" + stackname, profile)

    print pub_zk_hosts
    print pri_zk_hosts
    print elbs

def teardown_cli(args):
    teardown(args.stackname)

def teardown(stackname):
    kafka.teardown(stackname)
    zookeeper.teardown(stackname+"-zk")

def describe(args):
    pass

def parse_args():
    parser = argparse.ArgumentParser(description='Kafka full stack provision/teardown tool')
    commands = parser.add_subparsers(help="commands")

    parser1 = commands.add_parser("provision")
    parser1.add_argument('-s', dest='stackname', type=str, default='kafka', help='stack name')
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