import random
from unittest import TestCase

from leframework.model.mediator import Mediator
from leframework.model.states.percentilebucketgenerator import PercentileBucketGenerator


class PercentileBucketsGeneratorTest(TestCase):

    def testExecuteWith5000(self):
        self.executeWithTestSize(5000)
    
    def testExecuteWith1370(self):
        self.executeWithTestSize(1370)

    
    def executeWithTestSize(self, testSize):
        pctBucketGenerator = PercentileBucketGenerator()
        mediator = Mediator()  
        mediator.scored = [] 
        mediator.target = []
        for _ in range(testSize):
            prob = random.random()
            mediator.scored.append(prob)
            if prob > 0.7:
                mediator.target.append(1 if random.random() > 0.1 else 0)
            elif prob > 0.5:
                mediator.target.append(1 if random.random() > 0.5 else 0)
            elif prob > 0.3:
                mediator.target.append(1 if random.random() > 0.2 else 0)
            else:
                mediator.target.append(1 if random.random() > 0.7 else 0)

        pctBucketGenerator.setMediator(mediator)
        pctBucketGenerator.execute()
        buckets = pctBucketGenerator.getJsonProperty()
        self.assertEquals(len(buckets), 100)
        
        for i in range(0, len(buckets)-1):
            self.assertEquals(buckets[i]["MinimumScore"], buckets[i+1]["MaximumScore"])
        
        
        
        
