'''
Created on Oct 17, 2014

@author: hliu
'''
from rest.restclient import sendPutRequest
from modeling.modelingjob import ModelingJob

class SubmitModel(ModelingJob):
    
    def __init__(self, classDict):
        super(self.__class__, self).__init__(classDict)
        self.modelDefinition = classDict["model_definition"]
        self.model = classDict["model"]
        
        self.longOpts = [key + "=" for key in self.modelDefinition.viewkeys()]
        self.longOpts.extend([key + "=" for key in self.model.viewkeys()])
        self.longOpts.remove("model_definition=")
        
    def generateConfiguration(self, opts, config=None):
        for opt, arg in opts:
            self.populateValueFromOption(opt, arg, self.modelDefinition)
            self.populateValueFromOption(opt, arg, self.model)
            
        algorithms = []
        for algorithmName in self.modelDefinition["algorithms"]:
            for algorithm in config:
                if algorithmName == algorithm["name"]: algorithms.append(algorithm) 

        self.modelDefinition["algorithms"] = algorithms
        self.modelDefinition["name"] = "Model Definition"
        self.model["model_definition"] = self.modelDefinition
        self.model["data_format"] = "avro"
        return self.model
    
    def __getFeatures__(self, model, restEndpointHost):
        model["features"] = sendPutRequest(model, "features", restEndpointHost)["elements"]
    
    def submitJob(self, model, restEndpointHost):
        self.__getFeatures__(model, restEndpointHost)
        return sendPutRequest(model, "submit", restEndpointHost)
    
