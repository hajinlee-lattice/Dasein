import httplib

def write_to_stack(server, environment, stack, key, value):
    key = "%s/%s/%s" % (environment, stack, key)
    _write_to_consul(server, key, value)

def remove_stack(server, stack):
    conn = httplib.HTTPConnection(server)
    conn.request("DELETE", "/v1/kv/%s?recurse" % stack)
    response = conn.getresponse()
    print response.status, response.reason

def _write_to_consul(server, key, value):
    conn = httplib.HTTPConnection(server)
    conn.request("PUT", "/v1/kv/%s" % key, value)
    response = conn.getresponse()
    print response.status, response.reason

def _remove_from_consul(server, key):
    conn = httplib.HTTPConnection(server)
    conn.request("DELETE", "/v1/kv/%s?recurse" % key)
    response = conn.getresponse()
    print response.status, response.reason