import urllib
import httplib
import json

import sys
from optparse import OptionParser,OptionGroup


def constructOptionParser():
    parser = OptionParser()

    # required options
    parser.add_option("-o", "--hostport", dest="hostport", default="http://bodcprodjty221.prod.lattice.local",
                      help='default = http://bodcprodjty221.prod.lattice.local')
    parser.add_option("-c", "--command", dest="command", help='list, add, remove, update')

    parser.add_option_group(constructAddRemoveOptionGroup(parser))
    parser.add_option_group(constructUpdateOptionGroup(parser))
    return parser

def constructAddRemoveOptionGroup(parser):
    group = OptionGroup(parser, "Options for add/rm")
    group.add_option("-s", "--server", dest="server", help='the server to be added/removed')
    return group

def constructUpdateOptionGroup(parser):
    group = OptionGroup(parser, "Options for update")
    group.add_option("-S", "--servers", dest="servers", help='the list of available servers')
    group.add_option("-d", "--default", dest="default", help='the default server')
    return group

class CmdHandler:
    def __init__(self, options):
        self.protocal, self.host = options['hostport'].split('://')
        if self.protocal.lower() == 'https':
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
        self.servers = options['servers']
        self.default = options['default']

    def check_conn(self):
        sys.stdout.write("Pinging host %s://%s ... " % (self.protocal, self.host))
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
            'update': self.update
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
        if not self.add_server_to_options():
            return

        self.conn.request("PUT", "/admin/internal/services/options?component=VisiDBDL", json.dumps(self.node),
                          self.headers)
        response = self.conn.getresponse()
        print response.status, response.reason, "\n", response.read(), "\n"

        if response.status == 200:
            self.list()

    def remove(self):
        if self.server is None:
            print "Please specify the server name to be added!"
            return

        sys.stdout.write("Removing %s to the server list in ZK ... " % self.server)
        if not self.rm_server_from_options():
            return

        self.conn.request("PUT", "/admin/internal/services/options?component=VisiDBDL", json.dumps(self.node),
                          self.headers)
        response = self.conn.getresponse()
        print response.status, response.reason, "\n", response.read(), "\n"

        if response.status == 200:
            self.list()

    def update(self):
        if self.servers is None and self.default is None:
            print "Please specify the server list or the default server!"
            return

        sys.stdout.write("Updating the servers list and default server in ZK ... ")
        if not self.update_servers_and_default():
            return

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

    def add_server_to_options(self):
        if self.node is None:
            self.refresh()

        options = set(self.node['Options'])
        options.add(self.server)
        self.node['Options'] = list(options)
        return True

    def rm_server_from_options(self):
        if self.node is None:
            self.refresh()

        options = set(self.node['Options'])

        if self.server not in options:
            print "ERROR: %s is not in the current server list!" % self.server
            return False

        options.remove(self.server)
        self.node['Options'] = list(options)
        return True

    def update_servers_and_default(self):
        if self.node is None:
            self.refresh()

        if self.servers is not None:
            self.node['Options'] = list(set(self.servers))

        if self.default is not None:
            self.node['DefaultOption'] = self.default

        return True

if __name__ == "__main__":
    parser = constructOptionParser()
    (options, _) = parser.parse_args()
    handler = CmdHandler(options.__dict__)

    if not handler.check_conn():
        print "Bad connection, quit ..."

    handler.execute()


