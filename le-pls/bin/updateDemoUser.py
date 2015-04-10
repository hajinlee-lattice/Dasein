from optparse import OptionParser
import urllib
import subprocess
import sys
import httplib
import json

def updateUser(params):
    print 'Connecting to the host ...'

    protocal, host = params['hostwithprotocal'].split('://')
    if protocal.lower() == 'https':
        conn = httplib.HTTPSConnection(host)
    else:
        conn = httplib.HTTPConnection(host)

    conn.request('GET', "/")
    response = conn.getresponse()
    print response.status, response.reason
    response.read()

    # print 'Updating the user ...'
    # headers = {"MagicAuthentication": "Security through obscurity!", "Content-Type": "application/json"}
    # qpars = 'tenants=["' + params["tenant"] + '"]&namepattern=' + urllib.quote_plus(params["username"])
    # data = {"AccessLevel": params["accesslevel"], "OldPassword": params["oldpassword"],
    #         "NewPassword": params["newpassword"]}
    # conn.request('PUT', "/pls/internal/users?" + qpars, json.dumps(data), headers=headers)
    # response = conn.getresponse()
    # print response.status, response.reason
    # print response.read()

    conn.close()

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-o", "--hostwithprotocal", dest="hostwithprotocal", default="https://app.lattice-engines.com",
                      help='default = https://app.lattice-engines.com')
    parser.add_option("-p", "--port", dest="port", default=8080, help="used only for http. default = 8080")
    parser.add_option("-t", "--tenant", dest="tenant")
    parser.add_option("-u", "--username", dest="username")
    parser.add_option("-l", "--accesslevel", dest="accesslevel", default="EXTERNAL_USER", help="default = EXTERNAL_USER")
    parser.add_option("--oldpswd", dest="oldpassword")
    parser.add_option("--newpswd", dest="newpassword")
    (options, _) = parser.parse_args()
    updateUser(options.__dict__)