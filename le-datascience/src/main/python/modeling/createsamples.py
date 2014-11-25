'''
Created on Oct 17, 2014

@author: hliu
'''
from rest.restclient import sendPutRequest
from modeling.modelingjob import ModelingJob
import copy

class CreateSamples(ModelingJob):
    
    def __init__(self, classDict):
        super(self.__class__, self).__init__(classDict)
        self.samplingElement = classDict["samplingelement"]
        self.samplingConfiguration = classDict["samplingconfiguration"]
        
        self.longOpts = ["num_of_samples="]
        self.longOpts.extend([key + "=" for key in self.samplingConfiguration.viewkeys()])
        self.longOpts.remove("sampling_elements=")
        
    def generateConfiguration(self, opts, config=None):
        for opt, arg in opts:
            if str(opt).strip("--") == "num_of_samples": self.samplingConfiguration["sampling_elements"] = self.createSampleElements(int(arg))
            else: self.populateValueFromOption(opt, arg, self.samplingConfiguration) 
        return self.samplingConfiguration
        
    def submitJob(self, samplingConfiguration, restEndpointHost):
        for hdfsFile in self.listFiles("/user/s-analytics/customers/" + samplingConfiguration["customer"] + "/data/" + samplingConfiguration["table"] + "/samples"):
            if hdfsFile.endswith(".avro"):
                print "Samples have already been successfully created!"
                return "Passed" 
        return sendPutRequest(samplingConfiguration, "createSamples", restEndpointHost)
    
    def createSampleElements(self, numOfSamples):
        samplingElements = []
        value = 100 / numOfSamples
        for i in range(numOfSamples - 1):
            samplingElement = copy.deepcopy(self.samplingElement)
            samplingElement["name"] = "s" + str(i)
            samplingElement["percentage"] = value
            value *= i + 2
            samplingElements.append(samplingElement)
        samplingElement = copy.deepcopy(self.samplingElement)
        samplingElement["name"] = "all"
        samplingElement["percentage"] = 100
        samplingElements.append(samplingElement)
        return samplingElements
        
