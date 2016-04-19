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
        scoredSorted = mediator.allDataPreTransform[[mediator.schema["reserved"]["score"]]]
        scoredSorted.sort([mediator.schema["reserved"]["score"]], axis=0, ascending=False, inplace=True)
        scoredSortedLen = scoredSorted.shape[0]
        numElementsInBucket = int(scoredSortedLen/100.0)

        i = 0
        indexForMin = i + numElementsInBucket - 1
        pct = 100
        self.percentileBuckets = []
        # This means that the test set length is less than 100
        if numElementsInBucket == 0:
            self.logger.info("Dataset length is less than 100 so no buckets can be created.")
        else:
            self.logger.info("Length of test set array = %d." % scoredSortedLen)
            self.logger.info("Bucket size = %d." % numElementsInBucket)
            while indexForMin < scoredSortedLen:
                self.createBucket(scoredSorted, scoredSortedLen, i, indexForMin, pct)
                pct = pct - 1
                i = indexForMin
                indexForMin = i + numElementsInBucket - 1
                if pct == 0:
                    self.percentileBuckets[len(self.percentileBuckets) - 1]["MinimumScore"] = 0.0
                    break;

        # Add Result to Mediator
        self.mediator.percentileBuckets = self.percentileBuckets

    def createBucket(self, scoredSorted, scoredSortedLen, i, indexForMin, pct):
        bucket = OrderedDict()
        bucket["Percentile"] = pct
        
        try:
            if indexForMin > scoredSortedLen:
                indexForMin = scoredSortedLen - 1
            bucket["MinimumScore"] = scoredSorted[self.mediator.schema["reserved"]["score"]].iloc[indexForMin]
            bucket["MaximumScore"] = scoredSorted[self.mediator.schema["reserved"]["score"]].iloc[i]
            self.percentileBuckets.append(bucket)
        except Exception:
            self.logger.info("Length of list = %d indexForMin = %d i = %d" % (scoredSortedLen, indexForMin, i))
        if bucket["MinimumScore"] == bucket["MaximumScore"]:
            self.mediator.messages.append("Bucket for percentile %d has the same min and max for the range of %f." % \
                            (pct, scoredSorted[self.mediator.schema["reserved"]["score"]].iloc[i]))

    @overrides(JsonGenBase)
    def getKey(self):
        return "PercentileBuckets"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.percentileBuckets
