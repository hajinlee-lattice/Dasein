import httplib
import json

import sys
from optparse import OptionParser, OptionGroup


def construct_option_parser():
    parser = OptionParser()
    parser.add_option("-o", "--hostport", dest="hostport", default="http://bodcprodjty221.prod.lattice.local",
                      help='default = http://bodcprodjty221.prod.lattice.local')
    parser.add_option("-c", "--command", dest="command", help='list, add, remove, setdefault')
    parser.add_option("-s", "--server", dest="server", help='the server to be added/removed, or set to be default')
    return parser

class CmdHandler:
    def __init__(self, options):
        self.protocol, self.host = options['hostport'].split('://')
        if self.protocol.lower() == 'https':
            self.conn = httplib.HTTPSConnection(self.host)
        else:
            self.conn = httplib.HTTPConnection(self.host)
        self.cmd = options['command']
        self.headers = {
            "MagicAuthentication": "Security through obscurity!",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        self.node = None

        self.server = options['server']

    def check_conn(self):
        sys.stdout.write("Pinging host %s://%s ... " % (self.protocol, self.host))
        self.conn.request('GET', "/")
        response = self.conn.getresponse()
        print response.status, response.reason, "\n"
        response.read()
        return response.status == 200

    def execute(self):
        cmd_map = {
            'list': self.list,
            'add': self.add,
            'remove': self.remove,
            'setdefault': self.set_default
        }
        if self.cmd in cmd_map:
            cmd_map[self.cmd]()
        else:
            print "Command", self.cmd, "is unknown"

    def list(self):
        print "Getting current configuration in ZK ..."
        self.refresh()
        print "Default: %s" % self.node['DefaultOption']
        print "Servers: %s" % ", ".join(self.node['Options'])

    def add(self):
        if self.server is None:
            print "Please specify the server name to be added! You can use -s <server_name> or --server=<server_name>"
            return

        sys.stdout.write("Adding %s to the server list in ZK ... " % self.server)
        if not self.update_node_by_addding_server():
            return

        self.executeHttpPut()

    def remove(self):
        if self.server is None:
            print "Please specify the server name to be removed!"
            return

        sys.stdout.write("Removing %s to the server list in ZK ... " % self.server)
        if not self.update_node_by_rm_server():
            return

        self.executeHttpPut()

    def set_default(self):
        if self.server is None:
            print "Please specify the default server!"
            return

        sys.stdout.write("Updating the default server in ZK ... ")
        if not self.update_node_by_default_server():
            return

        self.executeHttpPut()

    def executeHttpPut(self):
        self.conn.request("PUT", "/admin/internal/services/options?component=VisiDBDL", json.dumps(self.node),
                          self.headers)
        response = self.conn.getresponse()
        print response.status, response.reason, "\n", response.read(), "\n"

        if response.status == 200:
            self.list()

    def refresh(self):
        self.conn.request('GET', "/admin/internal/services/options?component=VisiDBDL", headers=self.headers)
        response = self.conn.getresponse()
        body = json.loads(response.read())
        nodes = filter(lambda n: n['Node'] == "/VisiDB/ServerName", body['Nodes'])

        if len(nodes) == 0:
            print "Cannot find the visidb server list in ZK."

        self.node = nodes[0]

    def update_node_by_addding_server(self):
        if self.node is None:
            self.refresh()

        options = set(self.node['Options'])
        options.add(self.server)
        self.node['Options'] = list(options)
        return True

    def update_node_by_rm_server(self):
        if self.node is None:
            self.refresh()

        options = set(self.node['Options'])

        if self.server not in options:
            print "ERROR: %s is not in the current server list." % self.server
            return False

        if len(options) <= 1:
            print "ERROR: Cannot remove the only server from the list."
            return False

        options.remove(self.server)
        self.node['Options'] = list(options)
        self.node['DefaultOption'] = list(options)[0]
        return True

    def update_node_by_default_server(self):
        if self.node is None:
            self.refresh()

        options = set(self.node['Options'])
        if self.server not in options:
            print "ERROR: %s is not in the current server list." % self.server
            return False

        self.node['DefaultOption'] = self.server

        return True

if __name__ == "__main__":
    parser = construct_option_parser()
    (options, _) = parser.parse_args()
    handler = CmdHandler(options.__dict__)

    if not handler.check_conn():
        print "Bad connection, quit ..."

    handler.execute()


