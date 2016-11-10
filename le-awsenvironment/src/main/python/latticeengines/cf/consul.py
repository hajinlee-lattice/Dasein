import base64
import httplib
import json


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