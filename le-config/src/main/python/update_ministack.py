import argparse
import base64
import httplib
import json
import os

NEW_SUFFIX=".new"
HAPROXY_KEY="HAProxy"
KEYS_TO_BE_UPDATED = [
    "AWS_ADMIN_ADDRESS",
    "AWS_MICROSERVICE_ADDRESS",
    "AWS_MATCHAPI_ADDRESS",
    "AWS_PLS_ADDRESS",
    "AWS_SCORINGAPI_ADDRESS",
    "AWS_PLAYMAKER_ADDRESS",
    "AWS_OAUTH_ADDRESS"
]

def main():
    args = parse_args()
    print "profile=%s, consul=%s" % (args.profile, args.consul)
    update_profile(args.profile, args.environment, args.stack, args.consul)

def update_profile(profile, environment, stack, consul):
    ip = read_from_stack(consul, environment, stack, HAPROXY_KEY)
    print "found haproxy ip %s for stack %s in %s" % (ip, stack, environment)
    with open(profile, "r") as fin:
        with open(profile + NEW_SUFFIX, "w") as fout:
            for line in fin:
                if len(line.strip()) > 0 and ('#' != line.strip()[0]):
                    key = line.strip().replace('\n', '').split('=')[0]
                    value = line.strip().replace('\n', '')[len(key) + 1:]
                    if key in KEYS_TO_BE_UPDATED:
                        value = "http://%s" % ip
                        if key == "AWS_PLAYMAKER_ADDRESS":
                            value += "/api"
                        if key == "AWS_OAUTH_ADDRESS":
                            value += "/oauth2"
                    fout.write("%s=%s\n" % (key, value))
                else:
                    fout.write(line)
    os.rename(profile + NEW_SUFFIX, profile)


def write_to_stack(server, environment, stack, key, value):
    key = "%s/%s/%s" % (environment, stack, key)
    _write_to_consul(server, key, value)

def remove_stack(server, stack):
    conn = httplib.HTTPConnection(server)
    conn.request("DELETE", "/v1/kv/%s?recurse" % stack)
    response = conn.getresponse()
    print response.status, response.reason

def read_from_stack(server, environment, stack, key):
    key = "%s/%s/%s" % (environment, stack, key)
    return _read_from_consul(server, key)

def _write_to_consul(server, key, value):
    conn = httplib.HTTPConnection(server)
    conn.request("PUT", "/v1/kv/%s" % key, value)
    response = conn.getresponse()
    print response.status, response.reason

def _read_from_consul(server, key):
    conn = httplib.HTTPConnection(server)
    conn.request("GET", "/v1/kv/%s" % key)
    response = conn.getresponse()
    print response.status, response.reason
    body = response.read()
    return base64.b64decode(json.loads(body)[0]["Value"])

def _remove_from_consul(server, key):
    conn = httplib.HTTPConnection(server)
    conn.request("DELETE", "/v1/kv/%s?recurse" % key)
    response = conn.getresponse()
    print response.status, response.reason

def parse_args():
    parser = argparse.ArgumentParser(description='Replace tokens in properties')
    parser.add_argument('-e', dest='environment', type=str, default='devcluster', choices=['devcluster', 'qacluster','prodcluster'], help='environment')
    parser.add_argument('-s', dest='stack', type=str, required=True, help='the LE_STACK to be created')
    parser.add_argument('-c', dest='consul', type=str, required=True, help='consul server address')
    parser.add_argument('-p', dest='profile', type=str, required=True,
                        help='the stack profile file to be used to replace tokens')
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()