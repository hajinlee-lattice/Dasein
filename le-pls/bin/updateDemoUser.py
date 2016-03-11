from optparse import OptionParser
import urllib
import httplib
import json
import subprocess

def updateUser(params):
    print 'Connecting to the host ...'

    print 'Updating the user ...'
    qpars = 'tenant=' + urllib.quote_plus(params["tenant"]) + '&username=' + urllib.quote_plus(params["username"])
    data = {"AccessLevel": params["accesslevel"], "OldPassword": params["oldpassword"],
             "NewPassword": params["newpassword"]}

    payload = json.dumps(data)
    fullUrl = params['hostwithprotocal'] + "/pls/admin/users?" + qpars
    cmd = ["curl", "--insecure", "-H", "Content-Type: application/json", "-H",
           "MagicAuthentication: Security through obscurity!", "-X", "PUT",
           "-d",  payload, fullUrl]
    subprocess.call(cmd)

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-o", "--hostwithprotocal", dest="hostwithprotocal", default="https://app.lattice-engines.com",
                      help='default = https://app.lattice-engines.com')
    parser.add_option("-t", "--tenant", dest="tenant")
    parser.add_option("-u", "--username", dest="username")
    parser.add_option("-l", "--accesslevel", dest="accesslevel", default="EXTERNAL_USER", help="default = EXTERNAL_USER")
    parser.add_option("--oldpswd", dest="oldpassword")
    parser.add_option("--newpswd", dest="newpassword")
    (options, _) = parser.parse_args()
    updateUser(options.__dict__)