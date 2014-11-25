'''
Created on Oct 17, 2014

@author: hliu
'''

from rest.restclient import sendPutRequest
from modeling.modelingjob import ModelingJob

class LoadData(ModelingJob):
    
    def __init__(self, classDict):
        super(self.__class__, self).__init__(classDict)
        self.creds = classDict["creds"]
        self.loadConfiguration = classDict["loadconfiguration"]
        
        self.longOpts = [key + "=" for key in self.creds.viewkeys()]
        self.longOpts.extend([key + "=" for key in self.loadConfiguration.viewkeys()])
        self.longOpts.remove("creds=")
        
    def generateConfiguration(self, opts, config=None):
        for opt, arg in opts:
            self.populateValueFromOption(opt, arg, self.creds)
            self.populateValueFromOption(opt, arg, self.loadConfiguration)
        self.loadConfiguration["creds"] = self.creds
        return self.loadConfiguration
    
    def submitJob(self, loadConfiguration, restEndpointHost):
        hdfsFiles = self.listFiles("/user/s-analytics/customers/" + loadConfiguration["customer"] + "/data/" + loadConfiguration["table"])
        for hdfsFile in hdfsFiles:
            if hdfsFile.endswith(".avro"):
                print "Data has already been successfully loaded!"
                return "Passed"
        return sendPutRequest(loadConfiguration, "load", restEndpointHost)
    
