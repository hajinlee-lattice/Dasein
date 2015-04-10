#!/usr/bin/python

from optparse import OptionParser
import urllib
import subprocess
import sys
import httplib

def deleteUser(params):
    print 'Connecting to the host ...'
    protocal, host = params["host"].split('://')
    if protocal=='http':
        conn = httplib.HTTPConnection(host, int(params["port"]))
    else:
        conn = httplib.HTTPSConnection(host)
    conn.request('GET', "/")
    response = conn.getresponse()
    print response.status, response.reason
    response.read()

    print 'Deleting the user ...'
    headers = {"MagicAuthentication": "Security through obscurity!"}
    qpars = 'tenants=["' + params["tenant"] + '"]&namepattern=' + urllib.quote_plus(params["username"])
    conn.request('DELETE', "/pls/internal/users?" + qpars, headers=headers)
    response = conn.getresponse()
    print response.status, response.reason
    print response.read()

    conn.close()

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-o", "--host", dest="host", default="https://app.lattice-engines.com", help='default = https://app.lattice-engines.com')
    parser.add_option("-p", "--port", dest="port", default=8080, help="used only for http connection, default = 8080")
    parser.add_option("-t", "--tenant", dest="tenant")
    parser.add_option("-u", "--username", dest="username")	
    (options, _) = parser.parse_args()
 
    deleteUser(options.__dict__)
