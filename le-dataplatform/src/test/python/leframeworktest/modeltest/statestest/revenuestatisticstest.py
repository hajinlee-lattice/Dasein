import numpy as np

from leframeworktest.modeltest.statestest.scoretargetbase import ScoreTargetBase
from leframework.model.states.rocgenerator import ROCGenerator
from leframework.model.states.revenuestatistics import RevenueStatistics

class RevenueStatisticsGeneratorTest(ScoreTargetBase):

    def testGenerateRocScore(self):
        generator = RevenueStatistics()
        scoreTarget = [(0.5, 1), (0.5, 0), (0.5, 1), (0.5, 0), (0.5, 1), (0.5, 0)]
        revenue = [100, 200, 300, 400, 500, 600]
        predictedRevenue = [200, 300, 400, 500, 600, 700]
        self.loadMediator(generator, scoreTarget)
        mediator = generator.getMediator()
        mediator.schema["reserved"]["predictedrevenue"] = "PredictedRevenue"
        mediator.data['PredictedRevenue'] = predictedRevenue
        mediator.revenueColumn = "Revenue"
        mediator.data['Revenue'] = revenue
        
        generator.execute()

        self.assertEquals(mediator.revenueStatistics, [('topBucketByRevenue_PercentOverlap', 1.0), ('topBucketByRevenue_PercentTopRevenue', 1.0), ('topBucketByComboScore_PercentOverlap', 1.0), ('topBucketByComboScore_PercentTopRevenue', 1.0), ('revenuePrediction_L1Error', 0.0), ('revenuePrediction_SDError', 0.0), ('revenuePrediction_R2', 1.0)])
