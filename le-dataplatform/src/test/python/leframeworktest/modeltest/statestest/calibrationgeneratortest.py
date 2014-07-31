from testbase import TestBase
import random

from leframework.model.states.calibrationgenerator import CalibrationGenerator
from leframework.model.mediator import Mediator
class CalibrationGeneratorTest(TestBase):

    
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
        
        probRange = calibrationGenerator.mediator.probRange
        for i in range(len(probRange)-1):
            self.assertTrue(probRange[i] > probRange[i+1], "Probability conflict!")