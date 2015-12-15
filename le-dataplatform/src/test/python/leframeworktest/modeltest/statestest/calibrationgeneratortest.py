import random

from leframeworktest.modeltest.statestest.scoretargetbase import ScoreTargetBase
from leframework.model.states.calibrationgenerator import CalibrationGenerator
from leframework.model.states.calibrationwidthgenerator import CalibrationWithWidthGenerator

class CalibrationGeneratorTest(ScoreTargetBase):

    def testExecute(self):
        test_size = 5000

        generator = CalibrationGenerator()

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

        self.loadMediator(generator, scoreTarget)
        generator.execute()

        probRange = generator.mediator.probRange
        for i in range(len(probRange)-1):
            self.assertTrue(probRange[i] > probRange[i+1], "Probability conflict!" + str(probRange[i]) + "   " + str(probRange[i+1]))
            
        # Test new calibrition algorithm    
        generator = CalibrationWithWidthGenerator()
        self.loadMediator(generator, scoreTarget)
        generator.execute()

        probRange = generator.mediator.probRange
        for i in range(len(probRange)-1):
            self.assertTrue(probRange[i] > probRange[i+1], "Probability conflict!" + str(probRange[i]) + "   " + str(probRange[i+1]))

