from collections import OrderedDict
from sklearn import metrics
from sklearn.metrics.cluster.supervised import entropy
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
        eventData = mediator.data[:, mediator.schema["targetIndex"]]
        for key, value in mediator.metadata[0].iteritems():
            if key + "_1" in mediator.schema["targets"]:
                continue
            predictors.append(self.generatePredictors(key, value, eventData))
        
        predictors = sorted(predictors, key = lambda x: [x["Name"]])
        self.summary["Predictors"] = predictors
    
    @overrides(JsonGenBase)
    def getKey(self):
        return "Summary"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.summary
    
    def __getCountWhereEventIsOne(self, predictorData, eventData):
        counter = lambda x,y: 1 if x == 1 and y == 1 else 0
        return sum(map(counter, predictorData, eventData))
    
    def generatePredictors(self, colname, metadata, eventData):
        elements = []

        attrLevelUncertaintyCoeff = 0
        for record in metadata:
            predictorData = self.__getPredictorVector(colname, record)
            element = OrderedDict()
            
            countForBandValue = sum(predictorData)
            
            # If a band value is not found, skip that predictor value
            if countForBandValue == 0:
                continue
            countForBandValueAndEventIsOne = self.__getCountWhereEventIsOne(predictorData, eventData)
            lift = float(countForBandValueAndEventIsOne)/float(countForBandValue)
            avgLift = float(sum(eventData))/float(len(eventData))
            element["CorrelationSign"] = 1 if lift > avgLift else -1
            element["Count"] = countForBandValue
            if record["Dtype"] == "BND":
                element["LowerInclusive"] = record["minV"]
            element["Name"] = str(uuid.uuid4())
            element["UncertaintyCoefficient"] = self.__uncertaintyCoefficientXgivenY(eventData, predictorData)
            attrLevelUncertaintyCoeff += element["UncertaintyCoefficient"]
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
        predictor["UncertaintyCoefficient"] = attrLevelUncertaintyCoeff
        return predictor
    
    def __getPredictorVector(self, colname, record):
        converter = None
        try:            
            if record["Dtype"] == "BND":
                columnData = self.mediator.data[:, self.mediator.schema["nameToFeatureIndex"][colname + "_Continuous"]]
                minV = record["minV"]
                maxV = record["maxV"]
                converter = lambda x: 1 if x >= minV and x < maxV else 0
                return map(converter, columnData)
            else:
                return self.mediator.data[:, self.mediator.schema["nameToFeatureIndex"][colname + "_" + record["columnvalue"]]]
        except:
            return self.mediator.data[:, 1]
        
        
    def __uncertaintyCoefficientXgivenY(self, x, y):
        '''
          Given y, what parts of x can we predict.
          In this case, x should be the event column, while y should be the predictor column-value
        '''
        return metrics.mutual_info_score(x, y)/entropy(x) 

