import random

from leframeworktest.modeltest.statestest.scoretargetbase import ScoreTargetBase
from leframework.model.states.calibrationgenerator import CalibrationGenerator
from leframework.model.states.bucketgenerator import BucketGenerator

class BucketGeneratorTest(ScoreTargetBase):
    
    def testExecute(self):
        test_size = 5000
        
        calibrationGenerator = CalibrationGenerator()

        scored = [] 
        target = []
        for _ in xrange(test_size):
            prob = random.random()
            scored.append(prob)
            if prob > 0.7:
                target.append(1 if random.random() > 0.1 else 0)
            elif prob > 0.5:
                target.append(1 if random.random() > 0.5 else 0)
            elif prob > 0.3:
                target.append(1 if random.random() > 0.2 else 0)
            else:
                target.append(1 if random.random() > 0.7 else 0)

        scoreTarget = zip(scored, target)

        self.loadMediator(calibrationGenerator, scoreTarget)
        calibrationGenerator.execute()
        
        # Generate buckets and get probRange and indexRange
        bucketGenerator = BucketGenerator()
        bucketGenerator.setMediator(calibrationGenerator.getMediator())
        bucketGenerator.execute()
        buckets = bucketGenerator.buckets
        probRange = bucketGenerator.mediator.probRange
        widthRange = bucketGenerator.mediator.widthRange
        
        # Assert that highest has no more than one-half of the leads of high
        highest = buckets[0]["Minimum"]
        high = buckets[1]["Minimum"]
        if highest != None:
            self.assertTrue(high != None, "High Bucket is empty!")
            highestIndex = probRange.index(highest)
            
            for i in range(len(probRange)):
                if probRange[i] < high:
                    highIndex = i-1 
           
            self.assertTrue(self.__getCumulativeWidth(widthRange, highestIndex) < 0.5*self.__getCumulativeWidth(widthRange, highIndex), "Invalid bucket size for highest!")
            
        
    def __getCumulativeWidth(self, widthRange, idx):   
        cumWidth = 0
        for i in range(idx+1):
            cumWidth += widthRange[i]
            
        return cumWidth
