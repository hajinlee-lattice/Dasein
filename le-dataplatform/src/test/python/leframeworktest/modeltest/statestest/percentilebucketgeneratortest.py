import random

from leframeworktest.modeltest.statestest.scoretargetbase import ScoreTargetBase
from leframework.model.states.percentilebucketgenerator import PercentileBucketGenerator

class PercentileBucketsGeneratorTest(ScoreTargetBase):

    def testExecuteWith5000(self):
        self.executeWithTestSize(5000)

    def testExecuteWith1370(self):
        self.executeWithTestSize(1370)

    def executeWithTestSize(self, testSize):
        generator = PercentileBucketGenerator()

        scored = [] 
        target = []
        for _ in xrange(testSize):
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

        buckets = generator.getJsonProperty()
        self.assertEquals(len(buckets), 100)

        for i in range(0, len(buckets)-1):
            self.assertEquals(buckets[i]["MinimumScore"], buckets[i+1]["MaximumScore"])
