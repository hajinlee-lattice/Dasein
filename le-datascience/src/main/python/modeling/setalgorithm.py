'''
Created on Oct 20, 2014

@author: hliu
'''

from modeling.modelingjob import ModelingJob

class SetAlgorithm(ModelingJob):
    def __init__(self, classDict):
        self.algorithm = classDict["algorithmbase"]
        
        self.longOpts = [key + "=" for key in self.algorithm.viewkeys()]
        
    def getLongOptions(self):
        return self.longOpts
        
    def generateConfiguration(self, opts, config=None):
        for opt, arg in opts:
            for key in self.algorithm.viewkeys():
                if key == str(opt).strip("--"):
                    if key == "container_properties": 
                        self.algorithm["container_properties"] = " ".join(arg.split(","))
                    elif key == "algorithm_properties":
                        self.algorithm["algorithm_properties"] = " ".join(arg.split(","))
                    else: self.algorithm[key] = arg
        self.algorithm["type"] = self.algorithm["type"] = {"RF":"randomForestAlgorithm", "DT":"decisionTreeAlgorithm", "LR":"logisticRegressionAlgorithm"}[self.algorithm["name"][:2].upper()]        
        return self.algorithm
    
    def submitJob(self, loadConfiguration, restEndpointHost):
        pass
        
