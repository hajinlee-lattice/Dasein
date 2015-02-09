import numpy as np

from leframeworktest.modeltest.statestest.scoretargetbase import ScoreTargetBase
from leframework.model.states.rocgenerator import ROCGenerator

class ROCGeneratorTest(ScoreTargetBase):

    def testGenerateRocScore(self):
        generator = ROCGenerator()
        scoreTarget = [(0.5, 1),(0.5, 0),(0.5, 1),(0.5, 0),(0.5, 1),(0.5, 0)]

        self.loadMediator(generator, scoreTarget)
        generator.execute()

        self.assertEquals(generator.getMediator().rocscore, 0.7)

    def testGenerateRocScoreWithFewPositiveEvents(self):
        generator = ROCGenerator()
        scoreData = np.loadtxt("scored.txt", delimiter=",")[:,1]
        targetData = np.loadtxt("target.txt")
        scoreTarget = zip(scoreData, targetData)

        self.loadMediator(generator, scoreTarget)
        generator.execute()

        self.assertFalse(generator.getMediator().rocscore == 0)
