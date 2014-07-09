from collections import OrderedDict
import logging

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State


class PercentileBucketGenerator(State, JsonGenBase):

    def __init__(self):
        State.__init__(self, "PercentileBucketGenerator")
        self.logger = logging.getLogger(name = 'percentilebucketgenerator')
    
    @overrides(State)
    def execute(self):
        mediator = self.getMediator()
        scored = mediator.scored
        scoredSorted = sorted(set(scored), reverse=True)
        scoredSortedLen = len(scoredSorted)
        numElementsInBucket = int(scoredSortedLen/100.0)
        
        i = 0
        indexForMin = i + numElementsInBucket - 1
        pct = 100
        self.percentileBuckets = []
        self.logger.info("Length of test set array = %d." % scoredSortedLen)
        self.logger.info("Bucket size = %d." % numElementsInBucket)
        while indexForMin < scoredSortedLen:
            self.createBucket(scoredSorted, i, indexForMin, pct)
            pct = pct - 1
            i = indexForMin
            indexForMin = i + numElementsInBucket - 1
            if pct == 0:
                self.percentileBuckets[len(self.percentileBuckets) - 1]["MinimumScore"] = 0.0
                break;
    
    def createBucket(self, scoredSorted, i, indexForMin, pct):
        bucket = OrderedDict()
        bucket["Percentile"] = pct
        bucket["MinimumScore"] = scoredSorted[indexForMin]
        bucket["MaximumScore"] = scoredSorted[i]
        self.percentileBuckets.append(bucket)

    @overrides(JsonGenBase)
    def getKey(self):
        return "PercentileBuckets"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.percentileBuckets
