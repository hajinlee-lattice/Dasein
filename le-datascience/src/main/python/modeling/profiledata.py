'''
Created on Oct 17, 2014

@author: hliu
'''

from rest.restclient import sendPutRequest
from modeling.modelingjob import ModelingJob

class ProfileData(ModelingJob):
    
    def __init__(self, classDict):
        super(self.__class__, self).__init__(classDict)
        self.dataProfileConfiguration = classDict["dataprofileconfiguration"]
        
        self.longOpts = [key + "=" for key in self.dataProfileConfiguration.viewkeys()]
        
    def generateConfiguration(self, opts, config=None):        
        for opt, arg in opts: self.populateValueFromOption(opt, arg, self.dataProfileConfiguration)
        return self.dataProfileConfiguration
    
    def submitJob(self, dataProfileConfiguration, restEndpointHost):
        return sendPutRequest(dataProfileConfiguration, "profile", restEndpointHost)
