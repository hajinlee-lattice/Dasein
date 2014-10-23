from collections import OrderedDict
import json
import logging
import numpy

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State


class Finalize(State):

    def __init__(self):
        State.__init__(self, "Finalize")
        self.logger = logging.getLogger(name='finalize')
    
    @overrides(State)
    def execute(self):
        self.writeJson(self.getMediator())
        self.writeScoredText(self.getMediator())
        self.modelPredictorsExtraction(self.getMediator())

    def writeScoredText(self, mediator):
        scored = mediator.scored
        # add the key data and append the scored data
        keyData = mediator.data[:, mediator.schema["keyColIndex"]].astype(str)
        eventData = mediator.data[:, mediator.schema["targetIndex"]].astype(str)
        scored = numpy.insert(keyData, len(keyData[0]), scored, axis=1)
        # write the scored data to file
        numpy.savetxt(mediator.modelLocalDir + "scored.txt", scored, delimiter=",", fmt="%s")
        # write the target data to file
        numpy.savetxt(mediator.modelLocalDir + "target.txt", eventData, delimiter=",", fmt="%s")
        
    def writeJson(self, mediator):
        stateMachine = self.getStateMachine()
        states = stateMachine.getStates()
        jsonDict = OrderedDict()
        for state in states:
            if isinstance(state, JsonGenBase):
                key = state.getKey()
                value = state.getJsonProperty()
                jsonDict[key] = value
        with open(mediator.modelLocalDir + "model.json", "wb") as fp:
            json.dump(jsonDict, fp)
            
    def modelPredictorsExtraction(self, mediator):
        modelJSONFilePath = mediator.modelLocalDir + "model.json"
        csvFilePath = mediator.modelLocalDir + "model.csv"
        with open (modelJSONFilePath, "r") as myfile:
            modelJSON = myfile.read().replace('\n', '')
            
        contentJSON = json.loads(modelJSON)
        if contentJSON["Summary"] is None:
            print "-------------------------------------------"
            print "No ModelSummary information in the model!!"
            print "-------------------------------------------"                
            return 1
        
        if contentJSON["Summary"]["Predictors"] is None:
            print "-------------------------------------------"
            print "No Predictors information in the model!!"
            print "-------------------------------------------"
            return 1
        
        with open (csvFilePath, "w") as csvFile:
            csvFile.write("Feature_Name,Predictive_Power,Bucket_Lower_Bound,Bucket_Upper_Bound,Bucket_Value,Bucket_Probability,Bucket_Lift,Bucket_Frequency,Total#Lead\n")
            for predictor in contentJSON["Summary"]["Predictors"]:
                totalCount = 0
                for predictorElement in predictor["Elements"]:    
                    if 'Name' in predictor:   
                        if predictor["Name"] is None:
                            csvFile.write("null")
                        else:
                            csvFile.write('"')                    
                            csvFile.write(str(predictor["Name"]).replace('"', '""'))
                            csvFile.write('"')                        
                    csvFile.write(",")
                
                    if 'UncertaintyCoefficient' in predictor:
                        if predictor["UncertaintyCoefficient"] is None:
                            csvFile.write("null")
                        else:                     
                            csvFile.write('"' + str(predictor["UncertaintyCoefficient"]) + '"')            
                    csvFile.write(",")           
    
                    length = 0
                    if predictorElement["Values"] is not None:
                        length = len(predictorElement["Values"])
                    
                    if (length == 0):
                        if predictorElement["LowerInclusive"] is None:
                            csvFile.write("NA")
                        else:
                            csvFile.write(str(predictorElement["LowerInclusive"]))    
                        csvFile.write(",")                         
                        
                        if predictorElement["UpperExclusive"] is None:
                            csvFile.write("NA")
                        else:
                            csvFile.write(str(predictorElement["UpperExclusive"]))    
                        csvFile.write(",")
                        csvFile.write(",")                    
                    else:
                        if (predictorElement["Values"])[length - 1] is None:
                            csvFile.write(",")                    
                            csvFile.write(",")    
                            csvFile.write("[null],")                    
                        else:
                            csvFile.write(",")                    
                            csvFile.write(",")
                            csvFile.write('"[')
                            for val in predictorElement["Values"]:    
                                if val is not None:
                                    csvFile.write('""')                            
                                    csvFile.write(str(val).replace('"', '""'))
                                    csvFile.write('""')
                                    if val != (predictorElement["Values"])[length - 1]:
                                        csvFile.write(';')                         
                            csvFile.write(']",')
                    
                    if 'UncertaintyCoefficient' in predictorElement:
                        if predictorElement["UncertaintyCoefficient"] is None:
                            csvFile.write("null")
                        else:                         
                            csvFile.write(str(predictorElement["UncertaintyCoefficient"]))            
                    csvFile.write(",") 
    
                    if 'Lift' in predictorElement:     
                        if predictorElement["Lift"] is None:
                            csvFile.write("null")
                        else:                    
                            csvFile.write(str(predictorElement["Lift"]))            
                    csvFile.write(",")             
                 
                    if 'Count' in predictorElement:
                        if predictorElement["Count"] is None:
                            csvFile.write("null")
                        else:     
                            csvFile.write(str(predictorElement["Count"]))        
                            totalCount = totalCount + predictorElement["Count"]                        
                    csvFile.write(",") 
    
                    csvFile.write(str(totalCount))                
                    csvFile.write("\n")
        
        # print "----------------------------------------------------------------------------"
        # print "successfully extracted predictors information to " + CSVFilePath
        # print "----------------------------------------------------------------------------"

