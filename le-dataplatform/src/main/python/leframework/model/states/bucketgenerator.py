from collections import OrderedDict
import logging

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State

class BucketGenerator(State, JsonGenBase):
    ''' Define magic numbers that need to be passed in as parameter   '''
    loThres = 0.8
    medThres = 1.2
    highThres = float(1)/3    # one-third of remaining width as highest
    type = 0    # 0 - probability, 1 - lift
    def __init__(self):
        State.__init__(self, "BucketGenerator")
        self.logger = logging.getLogger(name='bucketgenerator')
    
    @overrides(State)
    def execute(self):
        # Algorithm generates buckets from low to highest
        # Will reverse it in the end to comply with overall order
        buckets = []
        labels = ["Low", "Medium", "High", "Highest"]
        probRange = self.mediator.probRange   
        widthRange = self.mediator.widthRange

        averageProbability = self.mediator.averageProbability
        self.logger.info("0 - Probability, 1 - Lift input data type is: "+str(self.type))
        if self.type == 0: 
            buckets.append((None, self.loThres*averageProbability))
            buckets.append((buckets[0][1], self.medThres*averageProbability))
        else:
            buckets.append((None, self.loThres))
            buckets.append((buckets[0][1], self.medThres))
            
        hasHigh = False
        for i in range(len(probRange)):
            if probRange[i] > buckets[1][1]:
                hasHigh = True
            else:
                highSize = i
                break    
                
        if hasHigh:
            # split one-third into highest
            remainWidth = 0
            for i in range(highSize):
                remainWidth += widthRange[i]
            
            curWidth = 0
            for i in range(highSize):
                tmpWidth = widthRange[i]
                if (curWidth + tmpWidth) > self.highThres*remainWidth: 
                    if i != 0:
                        buckets.append((buckets[1][1],probRange[i-1]))
                        buckets.append((probRange[i-1], None))
                    else:
                        buckets.append((buckets[1][1],probRange[i]))
                        buckets.append((probRange[i], None))
                    break
                else:         
                    curWidth += tmpWidth
        else:
            # medium is the highest label, change its max to None
            self.logger.info("None of the calibrations are above medium range")
            buckets.append((buckets.pop()[0], None))   
            for i in range(len(labels)-2):
                buckets.append((None, None))
                    
        
        # generate buckets
        self.buckets = [] 
        buckets.reverse()
        labels.reverse() 
        for i in range(len(labels)):
            element = OrderedDict()
            element["Maximum"] = buckets[i][1]
            element["Minimum"] = buckets[i][0]
            element["Name"] = labels[i]
            element["Type"] = self.type        
            self.buckets.append(element)          
        
        # Pass buckets,highest to low, to segmentChart with bucket type 
        self.mediator.buckets = self.buckets 
        self.mediator.type = self.type
        
    @overrides(JsonGenBase)
    def getKey(self):
        return "Buckets"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.buckets
    