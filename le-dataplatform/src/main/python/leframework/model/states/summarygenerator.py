from collections import OrderedDict
import uuid

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State


class SummaryGenerator(State, JsonGenBase):
    
    def __init__(self):
        State.__init__(self, "SummaryGenerator")
    
    @overrides(State)
    def execute(self):
        mediator = self.mediator
        self.summary = dict()
        predictors = []
        
        for key, value in mediator.metadata.iteritems():
            if key == "P1_Event":
                continue
            predictors.append(self.generatePredictors(key, value))
        
        self.summary["Predictors"] = predictors
    
    @overrides(JsonGenBase)
    def getKey(self):
        return "Summary"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.summary
    
    def generatePredictors(self, colname, metadata):
        elements = []
        
        for record in metadata:
            element = OrderedDict()
            element["CorrelationSign"] = 1
            element["Count"] = 0
            if record["Dtype"] == "BND":
                element["LowerInclusive"] = record["minV"]
            element["Name"] = str(uuid.uuid4())
            element["UncertaintyCoefficient"] = 0
            if record["Dtype"] == "BND":
                element["UpperExclusive"] = record["maxV"]
            if record["Dtype"] == "BND":
                element["Value"] = None
            else:
                element["Value"] = record["columnvalue"]
            elements.append(element)
        
        predictor = OrderedDict()
        predictor["Elements"] = elements
        predictor["Name"] = colname
        predictor["UncertaintyCoefficient"] = 0
        return predictor
