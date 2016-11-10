import argparse
import httplib2
import json

_CLIENT = httplib2.Http()
_CONSUL = "http://localhost:8500"

def main():
    args = parse_args()
    args.func(args)

def get_ip(args):
    nodes = get_nodes_in_service(args.service)
    if len(nodes) > 0:
        print nodes[0]['Address']

def get_zk(args):
    nodes = get_nodes_in_service("zookeeper")
    print ','.join("%s:%s" % (node['Address'], node['ServicePort']) for node in nodes)

def get_nodes_in_service(service):
    _, content = _CLIENT.request(_CONSUL + "/v1/catalog/service/%s" % service, "GET")
    return json.loads(content)

def parse_args():
    parser = argparse.ArgumentParser(description='Consul cli tool')
    commands = parser.add_subparsers(help="commands")

    parser1 = commands.add_parser("ip")
    parser1.add_argument('-s', dest='service', type=str, required=True, help='service')
    parser1.set_defaults(func=get_ip)

    parser1 = commands.add_parser("zk")
    parser1.set_defaults(func=get_zk)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()