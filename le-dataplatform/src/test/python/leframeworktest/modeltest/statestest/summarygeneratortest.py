from leframework.model.states.summarygenerator import SummaryGenerator
from testbase import TestBase

import numpy as np


class SummaryGeneratorTest(TestBase):
    
    def setUp(self):
        self.sg = SummaryGenerator()

    def testExecute(self):
        self.assertEquals(self.sg.getName(), "SummaryGenerator")
        self.assertEquals(self.sg.getKey(), "Summary")
        
    def testGenerateRocScore(self):
        score = [(0.5, 1),(0.5, 0),(0.5, 1),(0.5, 0),(0.5, 1),(0.5, 0)]
        rocScore = self.sg.getRocScore(score)
        self.assertEquals(rocScore, 0.7)
        
    def testGenerateRocScoreWithFewPositiveEvents(self):
        eventData = np.loadtxt("target.txt")
        scoreData = np.loadtxt("scored.txt", delimiter=",")[:,1]
        
        rocScore = self.sg.getRocScore(zip(scoreData, eventData))
        print("ROC score = %f" % rocScore)
     
