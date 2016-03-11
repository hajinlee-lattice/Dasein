#!/usr/bin/python

from optparse import OptionParser
import urllib
import subprocess
import sys
import httplib
import json

def updateUser(params):
    print 'Connecting to the host ...'
    conn = httplib.HTTPConnection(params["host"], int(params["port"]))
    conn.request('GET', "/")
    response = conn.getresponse()
    print response.status, response.reason
    response.read()

    print 'Updating the user ...'
    headers = {"MagicAuthentication": "Security through obscurity!", "Content-Type": "application/json"}
    qpars = 'tenants=["' + params["tenant"] + '"]&namepattern=' + urllib.quote_plus(params["username"])
    data = {"AccessLevel": params["accesslevel"], "OldPassword": params["oldpassword"], "NewPassword": params["newpassword"]}
    conn.request('PUT', "/pls/internal/users?" + qpars, json.dumps(data), headers=headers)
    response = conn.getresponse()
    print response.status, response.reason
    print response.read()

    conn.close()

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-o", "--host", dest="host", default="app.lattice-engines.com", help='default = app.lattice-engines.com')
    parser.add_option("-p", "--port", dest="port", default=80, help="default = 80")
    parser.add_option("-t", "--tenant", dest="tenant")
    parser.add_option("-u", "--username", dest="username")
    parser.add_option("-l", "--accesslevel", dest="accesslevel", default="EXTERNAL_USER", help="default = EXTERNAL_USER")
    parser.add_option("--oldpswd", dest="oldpassword")
    parser.add_option("--newpswd", dest="newpassword")
    (options, _) = parser.parse_args()
 
    updateUser(options.__dict__)

