import argparse
import os
from kazoo.client import KazooClient

WSHOME = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print 'WSHOME=%s' % WSHOME

def main():
    args = parseCliArgs()

    zk = KazooClient(hosts='127.0.0.1:2181')
    zk.start()

    print 'bootstrap source db in zk'
    filename = 'source_dbs_qa.json' if args.qasourcedbs else 'source_dbs_dev.json'
    json_file = os.path.join(WSHOME, 'le-dev', 'testartifacts', 'zookeeper', filename)
    with open(json_file) as f:
        stack = os.environ['LE_STACK']
        node = "/Pods/Default/Services/PropData/Stacks/%s/DataSources/SourceDB" % stack
        print node
        value = f.read()
        print value
        zk.ensure_path(node)
        zk.set(node, value)

    zk.stop()


def parseCliArgs():
    parser = argparse.ArgumentParser(description='Deploy wars to local tomcat')
    parser.add_argument('--qa-source-dbs', dest='qasourcedbs', action='store_true', help='using sql servers in qa network')
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    main()