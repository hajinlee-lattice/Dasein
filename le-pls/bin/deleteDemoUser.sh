#!/usr/bin/python

from optparse import OptionParser
from urllib import urlencode
import subprocess
import sys

request = '{"Tenant": "%s", "UserRegistration": {"User":{"Username":"%s","Email":"%s","FirstName":"%s","LastName":"%s","PhoneNumber":"650-555-5555","Active":false,"Title":"DemoUser"},"Credentials":{"Username":"%s","Password":"%s"},"Validation":false}}'

class UserDeployer(object):
    
    def __init__(self,  hostPortWithProtocol, tenant, username):
        self.url = "%s/pls/admin/users?namepattern=%s&tenants=[\"%s\"]" % (hostPortWithProtocol, urlencode(username), tenant)
    
    def deleteUser(self):
        cmd = ["curl", "--insecure", "-H", "Content-Type: application/json", "-H", \
               "MagicAuthentication: Security through obscurity!", "-X", "DELETE", \
               self.url]
        print(subprocess.check_output(cmd))

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-o", "--hostPortWithProtocol", dest="hostPortWithProtocol")
    parser.add_option("-t", "--tenant", dest="tenant")
    parser.add_option("-u", "--user", dest="user")
    (options, _) = parser.parse_args()
 
    params = options.__dict__
    ud = UserDeployer(params["hostPortWithProtocol"], params["tenant"], params["user"])
    ud.createUser()