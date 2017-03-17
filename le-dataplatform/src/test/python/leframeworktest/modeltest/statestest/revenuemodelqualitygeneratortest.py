import random

from leframeworktest.modeltest.statestest.scoretargetbase import ScoreTargetBase
from leframework.model.states.revenuemodelqualitygenerator import RevenueModelQualityGenerator


class RevenueModelQualityGeneratorTest(ScoreTargetBase):

    def testRevenueModelQualityScore(self):
        generator = RevenueModelQualityGenerator()
        numPointsToGenerate = 10000
        aucTarget = 0.5
        self.assertTrue(aucTarget < 1.0 and aucTarget > 0.0)
        scoreTarget = [(0.5, 1) if random.random() > aucTarget else (0.5, 0) for _ in range(0, numPointsToGenerate)]
        revenue = [100 + (100 * i) for i in range(0, numPointsToGenerate)]
        predictedRevenue = [200 + (100 * i) for i in range(0, numPointsToGenerate)]
        random.seed(2)
        periodId = [random.randint(1,5) for i in range(0, numPointsToGenerate)]
        self.loadMediator(generator, scoreTarget)
        mediator = generator.getMediator()
        mediator.schema["reserved"]["predictedrevenue"] = "PredictedRevenue"
        mediator.data['PredictedRevenue'] = predictedRevenue
        mediator.revenueColumn = "Revenue"
        mediator.data['Revenue'] = revenue
        mediator.data['Period_ID'] = periodId

        generator.execute()

        # Check that modelquality field has the correct keys
        self.assertTrue(mediator.modelquality.keys(), ["eventScores", "modelScores"])
        self.assertTrue(mediator.modelquality["eventScores"].keys(), ["auc", "outputLiftCurve"])
        self.assertTrue(mediator.modelquality["expectedValueScores"].keys(), ["percTotalRev", "percMaxCount"])

        # The AUC should be around the AUCTarget and we should have 100 buckets
        modelQuality = generator.getJsonProperty()
        calculatedAUC = modelQuality["eventScores"]["allPeriods"]["auc"]
        self.assertTrue(calculatedAUC > aucTarget - 0.2 and
                         calculatedAUC < aucTarget + 0.2
                        , "AUC doesn't seem correct in ModelQuality")
        self.assertEquals(len(modelQuality["expectedValueScores"]["allPeriods"]["percTotalRev"]), 100)
