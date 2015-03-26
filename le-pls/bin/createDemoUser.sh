#!/usr/bin/python

from optparse import OptionParser
import subprocess
import sys

request = '{"Tenant": "%s", "UserRegistration": {"User":{"Username":"%s","Email":"%s","FirstName":"%s","LastName":"%s","PhoneNumber":"650-555-5555","Active":false,"Title":"DemoUser"},"Credentials":{"Username":"%s","Password":"%s"},"Validation":false}}'

class UserDeployer(object):
    
    def __init__(self,  hostPortWithProtocol, tenant, username, password, email, firstName, lastName):
        self.filledInRequest = request % (tenant, username, email, firstName, lastName, username, password)
        self.url = "%s/pls/admin/users" % hostPortWithProtocol
    
    def createUser(self):
        cmd = ["curl", "--insecure", "-H", "Content-Type: application/json", "-H", \
               "MagicAuthentication: Security through obscurity!", "-X", "POST", \
               "-d", self.filledInRequest, \
               self.url]
        print(subprocess.check_output(cmd))

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-o", "--hostPortWithProtocol", dest="hostPortWithProtocol")
    parser.add_option("-t", "--tenant", dest="tenant")
    parser.add_option("-u", "--user", dest="user")
    parser.add_option("-e", "--email", dest="email")
    parser.add_option("-f", "--firstName", dest="firstName")
    parser.add_option("-l", "--lastName", dest="lastName")
    (options, _) = parser.parse_args()
 
    params = options.__dict__
    ud = UserDeployer(params["hostPortWithProtocol"], params["tenant"], params["user"], "EETAlfvFzCdm6/t3Ro8g89vzZo6EDCbucJMTPhYgWiE=", params["email"], params["firstName"], params["lastName"])
    ud.createUser()