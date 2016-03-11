#!/usr/bin/env python

from optparse import OptionParser
import subprocess
import sys

request = '{"Identifier": "%s", "DisplayName": "%s"}'

class TenantDeployer(object):
    
    def __init__(self,  hostPortWithProtocol, tenantId, tenantName):
        self.filledInRequest = request % (tenantId, tenantName)
        self.tenantName = tenantName;
        self.url = "%s/pls/admin/tenants" % hostPortWithProtocol
    
    def createTenant(self):
        cmd = ["curl", "-k", "-H", "Content-Type: application/json", "-H", \
               "MagicAuthentication: Security through obscurity!", "-X", "POST", \
               "-d",  self.filledInRequest, \
               self.url]
        subprocess.call(cmd)
        

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-o", "--hostPortWithProtocol", dest="hostPortWithProtocol")
    parser.add_option("-t", "--tenantId", dest="tenantId")
    parser.add_option("-n", "--tenantName", dest="tenantName")
    (options, _) = parser.parse_args()
 
    params = options.__dict__
    td = TenantDeployer(params["hostPortWithProtocol"], params["tenantId"], params["tenantName"])
    td.createTenant()