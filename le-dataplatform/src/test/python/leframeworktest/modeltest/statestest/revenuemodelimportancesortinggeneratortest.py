import random

from leframework.model.states.revenuemodelimportancesortinggenerator import RevenueModelImportanceSortingGenerator
from leframeworktest.modeltest.statestest.scoretargetbase import ScoreTargetBase


class RevenueModelImportanceSortingGeneratorTest(ScoreTargetBase):

    def testRevenueModelImportanceSorting(self):
        generator = RevenueModelImportanceSortingGenerator()
        numPointsToGenerate = 10000
        aucTarget = 0.5
        self.assertTrue(aucTarget < 1.0 and aucTarget > 0.0)
        scoreTarget = [(0.5, 1) if random.random() > aucTarget else (0.5, 0) for _ in range(0, numPointsToGenerate)]
        revenue = [100 + (100 * i) for i in range(0, numPointsToGenerate)]
        predictedRevenue = [200 + (100 * i) for i in range(0, numPointsToGenerate)]
        self.loadMediator(generator, scoreTarget)
        mediator = generator.getMediator()
        mediator.schema["reserved"]["predictedrevenue"] = "PredictedRevenue"
        mediator.data['PredictedRevenue'] = predictedRevenue
        mediator.revenueColumn = "Revenue"
        mediator.data['Revenue'] = revenue

        # Generate Test Data
        mediator.schema["features"] = ["Ext_LEAccounta", "Product_17_RevenueRollingSum6b", "ProductUnitsc"]
        mediator.data['Ext_LEAccounta'] = [1000 + (100 * i) for i in range(0, numPointsToGenerate)]
        mediator.data['Product_17_RevenueRollingSum6b'] = [str(100 + (100 * i)) for i in range(0, numPointsToGenerate)]
        mediator.data['ProductUnitsc'] = [str("a" + str(100 + (100 * i))) for i in range(0, numPointsToGenerate)]
        # Set some part of the column to None
        mediator.data['ProductUnitsc'][:numPointsToGenerate / 4] = None

        generator.execute()

        # Check that ImportanceSorting field has keys and correct lift
        self.assertTrue(len(mediator.importancesorting.keys()) > 0)
        importanceSorting = generator.getJsonProperty()
        self.assertAlmostEqual(importanceSorting["Revenue"]["Product_17_RevenueRollingSum6b"][0],
                               1.07, delta=0.1)
        self.assertAlmostEqual(importanceSorting["DataCloud"]["Ext_LEAccounta"][0],
                               1.07, delta=0.1)
        self.assertAlmostEqual(importanceSorting["Units"]["ProductUnitsc"][0],
                               0.85, delta=0.1)
        # Check percentage of nulls
        self.assertAlmostEqual(importanceSorting["Units"]["ProductUnitsc"][2],
                               0.25, delta=0.1)

