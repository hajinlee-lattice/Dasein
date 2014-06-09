from unittest import TestCase
import random

from leframework.model.states.calibrationgenerator import CalibrationGenerator
from leframework.model.states.bucketgenerator import BucketGenerator
from leframework.model.mediator import Mediator

class BucketGeneratorTest(TestCase):
    
    def testExecute(self):
        test_size = 5000
        
        calibrationGenerator = CalibrationGenerator()
        mediator = Mediator()  
        mediator.scored = [] 
        mediator.target = []
        for i in range(test_size):
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
        
        calibrationGenerator.setMediator(mediator)
        calibrationGenerator.execute()
        
        # Generate buckets and get probRange and indexRange
        bucketGenerator = BucketGenerator()
        bucketGenerator.setMediator(mediator)
        bucketGenerator.execute()
        buckets = bucketGenerator.buckets
        probRange = bucketGenerator.mediator.probRange
        widthRange = bucketGenerator.mediator.widthRange
        
        # Assert that highest has no more than one-half of the leads of high
        highest = buckets[0]["Minimum"]
        high = buckets[1]["Minimum"]
        if highest != None:
            self.assertTrue(high!=None, "High Bucket is empty!")
            highestIndex = probRange.index(highest)
            
            for i in range(len(probRange)):
                if probRange[i]<high:
                    highIndex = i-1 
           
            self.assertTrue(self.__getCumulativeWidth(widthRange,highestIndex) < 0.5*self.__getCumulativeWidth(widthRange,highIndex),"Invalid bucket size for highest!" )
            
        
    def __getCumulativeWidth(self,widthRange,idx):   
        cumWidth = 0
        for i in range(idx+1):
            cumWidth += widthRange[i]
            
        return cumWidth