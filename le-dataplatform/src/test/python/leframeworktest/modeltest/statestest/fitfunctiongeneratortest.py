import json
from testbase import TestBase
from leframework.model.states.generatefitfunction import FitFunctionGenerator, RevenueFitFunctionGenerator, \
    EVFitFunctionGenerator
from leframework.model.mediator import Mediator


class FitFunctionGeneratorTest(TestBase):

    def setUp(self):
        self.fit = FitFunctionGenerator()
        self.revenuefit = RevenueFitFunctionGenerator()
        self.evfit = EVFitFunctionGenerator()

    def testExecute(self):
        mediator = Mediator()
        mediator.segmentations = self._loadModel1()
        self.fit.mediator = mediator
        self.assertEquals(self.fit.getName(), "FitFunctionGenerator")
        self.fit.execute()
        fit_function_parameters = self.fit.getMediator().fit_function_parameters
        print(fit_function_parameters)
        # validate alpha, beta, gamma, maxRate, they are
        # -44.897047433156146 185.04455602025496 62.5 0.3627450980392157
        self.assertAlmostEquals(fit_function_parameters['alpha'], -44.89704743315614, delta=1e-6)
        self.assertAlmostEquals(fit_function_parameters['beta'], 185.04455602025496, delta=1e-6)
        self.assertAlmostEquals(fit_function_parameters['gamma'], 62.5, delta=1e-6)
        self.assertAlmostEquals(fit_function_parameters['maxRate'], 0.3627450980392157, delta=1e-6)

    def testExecute1(self):
        mediator = Mediator()
        mediator.segmentations = self._loadModel12()
        self.fit.mediator = mediator
        self.assertEquals(self.fit.getName(), "FitFunctionGenerator")
        self.fit.execute()
        fit_function_parameters = self.fit.getMediator().fit_function_parameters
        print(fit_function_parameters)
        self.assertAlmostEquals(fit_function_parameters['alpha'], 0.0, delta=1e-6)
        self.assertAlmostEquals(fit_function_parameters['beta'], -1.7346010553881064, delta=1e-6)
        self.assertAlmostEquals(fit_function_parameters['gamma'], 0.0, delta=1e-6)
        self.assertAlmostEquals(fit_function_parameters['maxRate'], 0.1, delta=1e-6)

    def testExecute2(self):
        mediator = Mediator()
        mediator.segmentations = self._loadModel2()
        self.fit.mediator = mediator
        self.assertEquals(self.fit.getName(), "FitFunctionGenerator")
        self.fit.execute()
        fit_function_parameters = self.fit.getMediator().fit_function_parameters
        print(fit_function_parameters)
        self.assertAlmostEquals(fit_function_parameters['alpha'], -68.03777530752788, delta=1e-6)
        self.assertAlmostEquals(fit_function_parameters['beta'], 312.60132663666815, delta=1e-6)
        self.assertAlmostEquals(fit_function_parameters['gamma'], 100., delta=1e-6)
        self.assertAlmostEquals(fit_function_parameters['maxRate'], 0.3235294117647059, delta=1e-6)

    def testExecute5(self):
        mediator = Mediator()
        mediator.segmentations = self._loadModel5()
        self.fit.mediator = mediator
        self.assertEquals(self.fit.getName(), "FitFunctionGenerator")
        self.fit.execute()
        fit_function_parameters = self.fit.getMediator().fit_function_parameters
        print(fit_function_parameters)
        self.assertAlmostEquals(fit_function_parameters['alpha'], -1.4704657617528374, delta=1e-6)
        self.assertAlmostEquals(fit_function_parameters['beta'], -2.1588076595661319, delta=1e-6)
        self.assertAlmostEquals(fit_function_parameters['gamma'], -0.675, delta=1e-6)
        self.assertAlmostEquals(fit_function_parameters['maxRate'], 1.0, delta=1e-6)

    def testRevenueExecute(self):
        mediator = Mediator()
        mediator.revenueColumn = "revScore"
        mediator.revenuesegmentations = self._loadRevenueModel()
        self.revenuefit.mediator = mediator
        self.assertEquals(self.revenuefit.getName(), "RevenueFitFunctionGenerator")
        self.revenuefit.execute()
        fit_function_parameters = self.revenuefit.getMediator().revenue_fit_function_parameters
        print(fit_function_parameters)
        self.assertAlmostEquals(fit_function_parameters['alpha'], -0.77680134113756127, delta=1e-4)
        self.assertAlmostEquals(fit_function_parameters['beta'], 8.2573176966537787, delta=1e-4)
        self.assertAlmostEquals(fit_function_parameters['gamma'], -0.42890625, delta=1e-4)
        self.assertAlmostEquals(fit_function_parameters['maxRate'], 11018.719539321424, delta=1e-4)

    def testEVExecute(self):
        mediator = Mediator()
        mediator.revenueColumn = "revScore"
        mediator.evsegmentations = self._loadEVModel()
        self.evfit.mediator = mediator
        self.assertEquals(self.evfit.getName(), "EVFitFunctionGenerator")
        self.evfit.execute()
        fit_function_parameters = self.evfit.getMediator().ev_fit_function_parameters
        print(fit_function_parameters)
        self.assertAlmostEquals(fit_function_parameters['alpha'], -3.6538693790657564, delta=1e-4)
        self.assertAlmostEquals(fit_function_parameters['beta'], 9.7189012862926418, delta=1e-4)
        self.assertAlmostEquals(fit_function_parameters['gamma'], 1.5625, delta=1e-4)
        self.assertAlmostEquals(fit_function_parameters['maxRate'], 1758.5505459430826, delta=1e-4)

    def testBadEVExecute(self):
        mediator = Mediator()
        mediator.revenueColumn = "revScore"
        mediator.evsegmentations = None
        self.evfit.mediator = mediator
        self.assertEquals(self.evfit.getName(), "EVFitFunctionGenerator")
        self.evfit.execute()
        fit_function_parameters = self.evfit.getMediator().ev_fit_function_parameters
        print(fit_function_parameters)
        self.assertAlmostEquals(fit_function_parameters['alpha'], 0.0, delta=1e-4)
        self.assertAlmostEquals(fit_function_parameters['beta'], 0.0, delta=1e-4)
        self.assertAlmostEquals(fit_function_parameters['gamma'], 0.0, delta=1e-4)
        self.assertAlmostEquals(fit_function_parameters['maxRate'], 0.0, delta=1e-4)

    def _loadRevenueModel(self):
        jsonStr = '''
        {
    "RevenueSegmentations": [
 {
  "LeadSource": "ALL", 
  "Segments": [
   {
    "Score": 100, 
    "Count": 28.0, 
    "Sum": 308524.1471009999, 
    "Mean": 11018.719539321424
   }, 
   {
    "Score": 99, 
    "Count": 27.0, 
    "Sum": 237669.026279, 
    "Mean": 8802.556528851852
   }, 
   {
    "Score": 98, 
    "Count": 28.0, 
    "Sum": 205251.75523, 
    "Mean": 7330.419829642858
   }, 
   {
    "Score": 97, 
    "Count": 27.0, 
    "Sum": 168310.14231199998, 
    "Mean": 6233.708974518518
   }, 
   {
    "Score": 96, 
    "Count": 27.0, 
    "Sum": 148523.648203, 
    "Mean": 5500.87585937037
   }, 
   {
    "Score": 95, 
    "Count": 28.0, 
    "Sum": 134448.791416, 
    "Mean": 4801.742550571428
   }, 
   {
    "Score": 94, 
    "Count": 27.0, 
    "Sum": 117628.304874, 
    "Mean": 4356.603884222222
   }, 
   {
    "Score": 93, 
    "Count": 28.0, 
    "Sum": 111847.310672, 
    "Mean": 3994.546809714286
   }, 
   {
    "Score": 92, 
    "Count": 27.0, 
    "Sum": 99326.159269, 
    "Mean": 3678.7466395925926
   }, 
   {
    "Score": 91, 
    "Count": 27.0, 
    "Sum": 92240.822896, 
    "Mean": 3416.326773925926
   }, 
   {
    "Score": 90, 
    "Count": 28.0, 
    "Sum": 88738.72061399999, 
    "Mean": 3169.240021928571
   }, 
   {
    "Score": 89, 
    "Count": 27.0, 
    "Sum": 80780.56906099999, 
    "Mean": 2991.8729281851847
   }, 
   {
    "Score": 88, 
    "Count": 28.0, 
    "Sum": 79555.966523, 
    "Mean": 2841.284518678571
   }, 
   {
    "Score": 87, 
    "Count": 27.0, 
    "Sum": 72876.926579, 
    "Mean": 2699.145428851852
   }, 
   {
    "Score": 86, 
    "Count": 27.0, 
    "Sum": 69651.68760100001, 
    "Mean": 2579.692133370371
   }, 
   {
    "Score": 85, 
    "Count": 28.0, 
    "Sum": 69458.53793400002, 
    "Mean": 2480.6620690714294
   }, 
   {
    "Score": 84, 
    "Count": 27.0, 
    "Sum": 63946.628629000006, 
    "Mean": 2368.3936529259263
   }, 
   {
    "Score": 83, 
    "Count": 28.0, 
    "Sum": 64626.69784500002, 
    "Mean": 2308.0963516071433
   }, 
   {
    "Score": 82, 
    "Count": 27.0, 
    "Sum": 60447.513758, 
    "Mean": 2238.796805851852
   }, 
   {
    "Score": 81, 
    "Count": 27.0, 
    "Sum": 58680.17981999999, 
    "Mean": 2173.339993333333
   }, 
   {
    "Score": 80, 
    "Count": 28.0, 
    "Sum": 59130.22157799999, 
    "Mean": 2111.793627785714
   }, 
   {
    "Score": 79, 
    "Count": 27.0, 
    "Sum": 55269.97183400002, 
    "Mean": 2047.0359938518525
   }, 
   {
    "Score": 78, 
    "Count": 27.0, 
    "Sum": 53472.893015999995, 
    "Mean": 1980.477519111111
   }, 
   {
    "Score": 77, 
    "Count": 28.0, 
    "Sum": 53881.951979000005, 
    "Mean": 1924.3554278214287
   }, 
   {
    "Score": 76, 
    "Count": 27.0, 
    "Sum": 50565.05396399999, 
    "Mean": 1872.7797764444442
   }, 
   {
    "Score": 75, 
    "Count": 28.0, 
    "Sum": 50989.454989, 
    "Mean": 1821.051963892857
   }, 
   {
    "Score": 74, 
    "Count": 27.0, 
    "Sum": 48056.779559, 
    "Mean": 1779.8807244074076
   }, 
   {
    "Score": 73, 
    "Count": 27.0, 
    "Sum": 46960.225217, 
    "Mean": 1739.2676006296297
   }, 
   {
    "Score": 72, 
    "Count": 28.0, 
    "Sum": 47725.89698500001, 
    "Mean": 1704.4963208928573
   }, 
   {
    "Score": 71, 
    "Count": 27.0, 
    "Sum": 45058.736812, 
    "Mean": 1668.8421041481483
   }, 
   {
    "Score": 70, 
    "Count": 28.0, 
    "Sum": 45528.423198000004, 
    "Mean": 1626.0151142142859
   }, 
   {
    "Score": 69, 
    "Count": 27.0, 
    "Sum": 43098.847902, 
    "Mean": 1596.253626
   }, 
   {
    "Score": 68, 
    "Count": 27.0, 
    "Sum": 42184.00265600001, 
    "Mean": 1562.3704687407412
   }, 
   {
    "Score": 67, 
    "Count": 28.0, 
    "Sum": 43043.881292, 
    "Mean": 1537.2814747142857
   }, 
   {
    "Score": 66, 
    "Count": 27.0, 
    "Sum": 40683.252559, 
    "Mean": 1506.7871318148148
   }, 
   {
    "Score": 65, 
    "Count": 28.0, 
    "Sum": 41242.638032, 
    "Mean": 1472.9513582857144
   }, 
   {
    "Score": 64, 
    "Count": 27.0, 
    "Sum": 38841.325518, 
    "Mean": 1438.5676117777778
   }, 
   {
    "Score": 63, 
    "Count": 27.0, 
    "Sum": 38280.21473400001, 
    "Mean": 1417.7857308888892
   }, 
   {
    "Score": 62, 
    "Count": 28.0, 
    "Sum": 38997.653149, 
    "Mean": 1392.77332675
   }, 
   {
    "Score": 61, 
    "Count": 27.0, 
    "Sum": 37106.041066, 
    "Mean": 1374.297817259259
   }, 
   {
    "Score": 60, 
    "Count": 27.0, 
    "Sum": 36579.75394, 
    "Mean": 1354.8057014814815
   }, 
   {
    "Score": 59, 
    "Count": 28.0, 
    "Sum": 37258.206228999996, 
    "Mean": 1330.6502224642857
   }, 
   {
    "Score": 58, 
    "Count": 27.0, 
    "Sum": 35211.788674999996, 
    "Mean": 1304.1403212962962
   }, 
   {
    "Score": 57, 
    "Count": 28.0, 
    "Sum": 35958.975345, 
    "Mean": 1284.2491194642857
   }, 
   {
    "Score": 56, 
    "Count": 27.0, 
    "Sum": 33999.280425, 
    "Mean": 1259.2326083333332
   }, 
   {
    "Score": 55, 
    "Count": 27.0, 
    "Sum": 33370.090496, 
    "Mean": 1235.9292776296295
   }, 
   {
    "Score": 54, 
    "Count": 28.0, 
    "Sum": 33953.37057100001, 
    "Mean": 1212.6203775357146
   }, 
   {
    "Score": 53, 
    "Count": 27.0, 
    "Sum": 32202.469197000002, 
    "Mean": 1192.6840443333333
   }, 
   {
    "Score": 52, 
    "Count": 28.0, 
    "Sum": 32876.172455, 
    "Mean": 1174.14901625
   }, 
   {
    "Score": 51, 
    "Count": 27.0, 
    "Sum": 31157.786599000003, 
    "Mean": 1153.9920962592594
   }, 
   {
    "Score": 50, 
    "Count": 27.0, 
    "Sum": 30681.298199999997, 
    "Mean": 1136.3443777777777
   }, 
   {
    "Score": 49, 
    "Count": 28.0, 
    "Sum": 31118.863175000002, 
    "Mean": 1111.3879705357144
   }, 
   {
    "Score": 48, 
    "Count": 27.0, 
    "Sum": 29571.973621, 
    "Mean": 1095.2582822592592
   }, 
   {
    "Score": 47, 
    "Count": 28.0, 
    "Sum": 30294.219294000002, 
    "Mean": 1081.936403357143
   }, 
   {
    "Score": 46, 
    "Count": 27.0, 
    "Sum": 28776.294866, 
    "Mean": 1065.7886987407408
   }, 
   {
    "Score": 45, 
    "Count": 27.0, 
    "Sum": 28349.06142, 
    "Mean": 1049.965237777778
   }, 
   {
    "Score": 44, 
    "Count": 28.0, 
    "Sum": 28929.391259, 
    "Mean": 1033.1925449642856
   }, 
   {
    "Score": 43, 
    "Count": 27.0, 
    "Sum": 27457.207684999994, 
    "Mean": 1016.9336179629628
   }, 
   {
    "Score": 42, 
    "Count": 28.0, 
    "Sum": 27884.052612999996, 
    "Mean": 995.859021892857
   }, 
   {
    "Score": 41, 
    "Count": 27.0, 
    "Sum": 26497.576499999996, 
    "Mean": 981.391722222222
   }, 
   {
    "Score": 40, 
    "Count": 27.0, 
    "Sum": 26147.295153, 
    "Mean": 968.418339
   }, 
   {
    "Score": 39, 
    "Count": 28.0, 
    "Sum": 26726.369093999998, 
    "Mean": 954.5131819285714
   }, 
   {
    "Score": 38, 
    "Count": 27.0, 
    "Sum": 25425.859755, 
    "Mean": 941.6985094444445
   }, 
   {
    "Score": 37, 
    "Count": 27.0, 
    "Sum": 24999.857461999996, 
    "Mean": 925.9206467407406
   }, 
   {
    "Score": 36, 
    "Count": 28.0, 
    "Sum": 25553.158493000003, 
    "Mean": 912.6128033214287
   }, 
   {
    "Score": 35, 
    "Count": 27.0, 
    "Sum": 24256.305849, 
    "Mean": 898.3816981111112
   }, 
   {
    "Score": 34, 
    "Count": 28.0, 
    "Sum": 24826.889526, 
    "Mean": 886.6746259285713
   }, 
   {
    "Score": 33, 
    "Count": 27.0, 
    "Sum": 23555.766221, 
    "Mean": 872.435785962963
   }, 
   {
    "Score": 32, 
    "Count": 27.0, 
    "Sum": 23198.516644000003, 
    "Mean": 859.2043201481482
   }, 
   {
    "Score": 31, 
    "Count": 28.0, 
    "Sum": 23643.204805, 
    "Mean": 844.4001716071429
   }, 
   {
    "Score": 30, 
    "Count": 27.0, 
    "Sum": 22487.640423000004, 
    "Mean": 832.8755712222223
   }, 
   {
    "Score": 29, 
    "Count": 28.0, 
    "Sum": 22948.163194, 
    "Mean": 819.5772569285715
   }, 
   {
    "Score": 28, 
    "Count": 27.0, 
    "Sum": 21866.038626999998, 
    "Mean": 809.8532824814814
   }, 
   {
    "Score": 27, 
    "Count": 27.0, 
    "Sum": 21564.084734000004, 
    "Mean": 798.6698049629631
   }, 
   {
    "Score": 26, 
    "Count": 28.0, 
    "Sum": 22077.471325, 
    "Mean": 788.48111875
   }, 
   {
    "Score": 25, 
    "Count": 27.0, 
    "Sum": 20973.010029999998, 
    "Mean": 776.7781492592592
   }, 
   {
    "Score": 24, 
    "Count": 28.0, 
    "Sum": 21436.934219999996, 
    "Mean": 765.6047935714284
   }, 
   {
    "Score": 23, 
    "Count": 27.0, 
    "Sum": 20429.334988000002, 
    "Mean": 756.6420365925927
   }, 
   {
    "Score": 22, 
    "Count": 27.0, 
    "Sum": 20149.970415000003, 
    "Mean": 746.2952005555557
   }, 
   {
    "Score": 21, 
    "Count": 28.0, 
    "Sum": 20557.866565999997, 
    "Mean": 734.2095202142856
   }, 
   {
    "Score": 20, 
    "Count": 27.0, 
    "Sum": 19495.950096, 
    "Mean": 722.0722257777778
   }, 
   {
    "Score": 19, 
    "Count": 27.0, 
    "Sum": 19183.509678000002, 
    "Mean": 710.5003584444445
   }, 
   {
    "Score": 18, 
    "Count": 28.0, 
    "Sum": 19551.534918999994, 
    "Mean": 698.2691042499998
   }, 
   {
    "Score": 17, 
    "Count": 27.0, 
    "Sum": 18530.345149, 
    "Mean": 686.3090795925926
   }, 
   {
    "Score": 16, 
    "Count": 28.0, 
    "Sum": 18910.600482, 
    "Mean": 675.3785886428572
   }, 
   {
    "Score": 15, 
    "Count": 27.0, 
    "Sum": 17958.342258999994, 
    "Mean": 665.1237873703701
   }, 
   {
    "Score": 14, 
    "Count": 27.0, 
    "Sum": 17659.084794999995, 
    "Mean": 654.0401775925924
   }, 
   {
    "Score": 13, 
    "Count": 28.0, 
    "Sum": 18009.715692, 
    "Mean": 643.2041318571429
   }, 
   {
    "Score": 12, 
    "Count": 27.0, 
    "Sum": 17027.388208, 
    "Mean": 630.6440077037037
   }, 
   {
    "Score": 11, 
    "Count": 28.0, 
    "Sum": 17366.050537000003, 
    "Mean": 620.216090607143
   }, 
   {
    "Score": 10, 
    "Count": 27.0, 
    "Sum": 16549.869441, 
    "Mean": 612.9581274444444
   }, 
   {
    "Score": 9, 
    "Count": 27.0, 
    "Sum": 16250.410698000002, 
    "Mean": 601.867062888889
   }, 
   {
    "Score": 8, 
    "Count": 28.0, 
    "Sum": 16537.196044, 
    "Mean": 590.6141444285714
   }, 
   {
    "Score": 7, 
    "Count": 27.0, 
    "Sum": 15606.200425999998, 
    "Mean": 578.0074231851851
   }, 
   {
    "Score": 6, 
    "Count": 28.0, 
    "Sum": 15802.688342000001, 
    "Mean": 564.3817265
   }, 
   {
    "Score": 5, 
    "Count": 27.0, 
    "Sum": 14857.278484999999, 
    "Mean": 550.2695735185184
   }, 
   {
    "Score": 4, 
    "Count": 27.0, 
    "Sum": 14394.335344, 
    "Mean": 533.1235312592593
   }, 
   {
    "Score": 3, 
    "Count": 28.0, 
    "Sum": 14463.237247, 
    "Mean": 516.5441873928571
   }, 
   {
    "Score": 2, 
    "Count": 27.0, 
    "Sum": 13314.198987000002, 
    "Mean": 493.11848100000003
   }, 
   {
    "Score": 1, 
    "Count": 27.0, 
    "Sum": 12345.251378999998, 
    "Mean": 457.2315325555555
   }
  ]
 }
]
}
        '''
        obj = json.loads(jsonStr)
        return obj['RevenueSegmentations']

    def _loadModel5(self):
        jsonStr = '''
        {
  "Segmentations": [
    {
      "LeadSource": "All",
      "Segments": [
        {
          "Score": 100,
          "Count": 33,
          "Converted": 33
        },
        {
          "Score": 99,
          "Count": 33,
          "Converted": 33
        },
        {
          "Score": 98,
          "Count": 33,
          "Converted": 26
        },
        {
          "Score": 97,
          "Count": 33,
          "Converted": 26
        },
        {
          "Score": 96,
          "Count": 33,
          "Converted": 17
        },
        {
          "Score": 95,
          "Count": 33,
          "Converted": 14
        },
        {
          "Score": 94,
          "Count": 33,
          "Converted": 11
        },
        {
          "Score": 93,
          "Count": 33,
          "Converted": 15
        },
        {
          "Score": 92,
          "Count": 33,
          "Converted": 11
        },
        {
          "Score": 91,
          "Count": 33,
          "Converted": 11
        },
        {
          "Score": 90,
          "Count": 33,
          "Converted": 3
        },
        {
          "Score": 89,
          "Count": 33,
          "Converted": 3
        },
        {
          "Score": 88,
          "Count": 33,
          "Converted": 5
        },
        {
          "Score": 87,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 86,
          "Count": 33,
          "Converted": 1
        },
        {
          "Score": 85,
          "Count": 33,
          "Converted": 1
        },
        {
          "Score": 84,
          "Count": 33,
          "Converted": 1
        },
        {
          "Score": 83,
          "Count": 33,
          "Converted": 2
        },
        {
          "Score": 82,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 81,
          "Count": 33,
          "Converted": 2
        },
        {
          "Score": 80,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 79,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 78,
          "Count": 33,
          "Converted": 3
        },
        {
          "Score": 77,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 76,
          "Count": 33,
          "Converted": 1
        },
        {
          "Score": 75,
          "Count": 33,
          "Converted": 1
        },
        {
          "Score": 74,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 73,
          "Count": 33,
          "Converted": 5
        },
        {
          "Score": 72,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 71,
          "Count": 33,
          "Converted": 1
        },
        {
          "Score": 70,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 69,
          "Count": 33,
          "Converted": 2
        },
        {
          "Score": 68,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 67,
          "Count": 33,
          "Converted": 1
        },
        {
          "Score": 66,
          "Count": 33,
          "Converted": 1
        },
        {
          "Score": 65,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 64,
          "Count": 33,
          "Converted": 1
        },
        {
          "Score": 63,
          "Count": 33,
          "Converted": 1
        },
        {
          "Score": 62,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 61,
          "Count": 33,
          "Converted": 2
        },
        {
          "Score": 60,
          "Count": 33,
          "Converted": 1
        },
        {
          "Score": 59,
          "Count": 33,
          "Converted": 2
        },
        {
          "Score": 58,
          "Count": 33,
          "Converted": 2
        },
        {
          "Score": 57,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 56,
          "Count": 33,
          "Converted": 1
        },
        {
          "Score": 55,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 54,
          "Count": 33,
          "Converted": 1
        },
        {
          "Score": 53,
          "Count": 33,
          "Converted": 1
        },
        {
          "Score": 52,
          "Count": 33,
          "Converted": 2
        },
        {
          "Score": 51,
          "Count": 33,
          "Converted": 4
        },
        {
          "Score": 50,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 49,
          "Count": 33,
          "Converted": 2
        },
        {
          "Score": 48,
          "Count": 33,
          "Converted": 1
        },
        {
          "Score": 47,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 46,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 45,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 44,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 43,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 42,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 41,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 40,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 39,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 38,
          "Count": 33,
          "Converted": 1
        },
        {
          "Score": 37,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 36,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 35,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 34,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 33,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 32,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 31,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 30,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 29,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 28,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 27,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 26,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 25,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 24,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 23,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 22,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 21,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 20,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 19,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 18,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 17,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 16,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 15,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 14,
          "Count": 33,
          "Converted": 0
        },
        {
          "Score": 13,
          "Count": 32,
          "Converted": 0
        },
        {
          "Score": 12,
          "Count": 32,
          "Converted": 0
        },
        {
          "Score": 11,
          "Count": 32,
          "Converted": 0
        },
        {
          "Score": 10,
          "Count": 32,
          "Converted": 0
        },
        {
          "Score": 9,
          "Count": 32,
          "Converted": 0
        },
        {
          "Score": 8,
          "Count": 32,
          "Converted": 0
        },
        {
          "Score": 7,
          "Count": 32,
          "Converted": 0
        },
        {
          "Score": 6,
          "Count": 32,
          "Converted": 0
        },
        {
          "Score": 5,
          "Count": 32,
          "Converted": 0
        },
        {
          "Score": 4,
          "Count": 32,
          "Converted": 0
        },
        {
          "Score": 3,
          "Count": 32,
          "Converted": 0
        },
        {
          "Score": 2,
          "Count": 32,
          "Converted": 0
        },
        {
          "Score": 1,
          "Count": 32,
          "Converted": 0
        }
      ]
    }
  ]
}
        '''
        obj = json.loads(jsonStr)
        return obj['Segmentations']

    def _loadModel2(self):
        jsonStr = '''
        {
    "Segmentations": [
        {
            "LeadSource": "All", 
            "Segments": [
                {
                    "Score": 100, 
                    "Count": 102, 
                    "Converted": 33
                }, 
                {
                    "Score": 99, 
                    "Count": 102, 
                    "Converted": 20
                }, 
                {
                    "Score": 98, 
                    "Count": 102, 
                    "Converted": 31
                }, 
                {
                    "Score": 97, 
                    "Count": 102, 
                    "Converted": 28
                }, 
                {
                    "Score": 96, 
                    "Count": 102, 
                    "Converted": 22
                }, 
                {
                    "Score": 95, 
                    "Count": 102, 
                    "Converted": 18
                }, 
                {
                    "Score": 94, 
                    "Count": 102, 
                    "Converted": 36
                }, 
                {
                    "Score": 93, 
                    "Count": 102, 
                    "Converted": 17
                }, 
                {
                    "Score": 92, 
                    "Count": 102, 
                    "Converted": 21
                }, 
                {
                    "Score": 91, 
                    "Count": 102, 
                    "Converted": 18
                }, 
                {
                    "Score": 90, 
                    "Count": 102, 
                    "Converted": 22
                }, 
                {
                    "Score": 89, 
                    "Count": 102, 
                    "Converted": 18
                }, 
                {
                    "Score": 88, 
                    "Count": 102, 
                    "Converted": 17
                }, 
                {
                    "Score": 87, 
                    "Count": 102, 
                    "Converted": 15
                }, 
                {
                    "Score": 86, 
                    "Count": 102, 
                    "Converted": 22
                }, 
                {
                    "Score": 85, 
                    "Count": 102, 
                    "Converted": 18
                }, 
                {
                    "Score": 84, 
                    "Count": 102, 
                    "Converted": 13
                }, 
                {
                    "Score": 83, 
                    "Count": 102, 
                    "Converted": 18
                }, 
                {
                    "Score": 82, 
                    "Count": 102, 
                    "Converted": 15
                }, 
                {
                    "Score": 81, 
                    "Count": 102, 
                    "Converted": 10
                }, 
                {
                    "Score": 80, 
                    "Count": 102, 
                    "Converted": 15
                }, 
                {
                    "Score": 79, 
                    "Count": 102, 
                    "Converted": 14
                }, 
                {
                    "Score": 78, 
                    "Count": 102, 
                    "Converted": 14
                }, 
                {
                    "Score": 77, 
                    "Count": 102, 
                    "Converted": 13
                }, 
                {
                    "Score": 76, 
                    "Count": 102, 
                    "Converted": 9
                }, 
                {
                    "Score": 75, 
                    "Count": 102, 
                    "Converted": 9
                }, 
                {
                    "Score": 74, 
                    "Count": 102, 
                    "Converted": 5
                }, 
                {
                    "Score": 73, 
                    "Count": 102, 
                    "Converted": 4
                }, 
                {
                    "Score": 72, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 71, 
                    "Count": 102, 
                    "Converted": 1
                }, 
                {
                    "Score": 70, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 69, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 68, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 67, 
                    "Count": 102, 
                    "Converted": 4
                }, 
                {
                    "Score": 66, 
                    "Count": 102, 
                    "Converted": 4
                }, 
                {
                    "Score": 65, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 64, 
                    "Count": 102, 
                    "Converted": 3
                }, 
                {
                    "Score": 63, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 62, 
                    "Count": 102, 
                    "Converted": 1
                }, 
                {
                    "Score": 61, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 60, 
                    "Count": 102, 
                    "Converted": 1
                }, 
                {
                    "Score": 59, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 58, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 57, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 56, 
                    "Count": 102, 
                    "Converted": 1
                }, 
                {
                    "Score": 55, 
                    "Count": 102, 
                    "Converted": 3
                }, 
                {
                    "Score": 54, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 53, 
                    "Count": 102, 
                    "Converted": 1
                }, 
                {
                    "Score": 52, 
                    "Count": 102, 
                    "Converted": 1
                }, 
                {
                    "Score": 51, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 50, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 49, 
                    "Count": 102, 
                    "Converted": 1
                }, 
                {
                    "Score": 48, 
                    "Count": 102, 
                    "Converted": 1
                }, 
                {
                    "Score": 47, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 46, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 45, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 44, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 43, 
                    "Count": 102, 
                    "Converted": 1
                }, 
                {
                    "Score": 42, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 41, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 40, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 39, 
                    "Count": 102, 
                    "Converted": 1
                }, 
                {
                    "Score": 38, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 37, 
                    "Count": 102, 
                    "Converted": 1
                }, 
                {
                    "Score": 36, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 35, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 34, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 33, 
                    "Count": 101, 
                    "Converted": 2
                }, 
                {
                    "Score": 32, 
                    "Count": 101, 
                    "Converted": 1
                }, 
                {
                    "Score": 31, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 30, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 29, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 28, 
                    "Count": 101, 
                    "Converted": 2
                }, 
                {
                    "Score": 27, 
                    "Count": 101, 
                    "Converted": 1
                }, 
                {
                    "Score": 26, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 25, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 24, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 23, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 22, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 21, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 20, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 19, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 18, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 17, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 16, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 15, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 14, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 13, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 12, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 11, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 10, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 9, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 8, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 7, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 6, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 5, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 4, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 3, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 2, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 1, 
                    "Count": 101, 
                    "Converted": 0
                }
            ]
        }
    ] 
}
        '''
        obj = json.loads(jsonStr)
        return obj['Segmentations']

    def _loadModel12(self):
        jsonStr = '''
        {
        "Segmentations": [
        {
            "LeadSource": "All", 
            "Segments": [
                {
                    "Score": 100, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 99, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 98, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 97, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 96, 
                    "Count": 1, 
                    "Converted": 1
                }, 
                {
                    "Score": 95, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 94, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 93, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 92, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 91, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 90, 
                    "Count": 1, 
                    "Converted": 1
                }, 
                {
                    "Score": 89, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 88, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 87, 
                    "Count": 1, 
                    "Converted": 1
                }, 
                {
                    "Score": 86, 
                    "Count": 1, 
                    "Converted": 1
                }, 
                {
                    "Score": 85, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 84, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 83, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 82, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 81, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 80, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 79, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 78, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 77, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 76, 
                    "Count": 1, 
                    "Converted": 1
                }, 
                {
                    "Score": 75, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 74, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 73, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 72, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 71, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 70, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 69, 
                    "Count": 1, 
                    "Converted": 1
                }, 
                {
                    "Score": 68, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 67, 
                    "Count": 1, 
                    "Converted": 1
                }, 
                {
                    "Score": 66, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 65, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 64, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 63, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 62, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 61, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 60, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 59, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 58, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 57, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 56, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 55, 
                    "Count": 1, 
                    "Converted": 1
                }, 
                {
                    "Score": 54, 
                    "Count": 1, 
                    "Converted": 1
                }, 
                {
                    "Score": 53, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 52, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 51, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 50, 
                    "Count": 1, 
                    "Converted": 0
                }, 
                {
                    "Score": 49, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 48, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 47, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 46, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 45, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 44, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 43, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 42, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 41, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 40, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 39, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 38, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 37, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 36, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 35, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 34, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 33, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 32, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 31, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 30, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 29, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 28, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 27, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 26, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 25, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 24, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 23, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 22, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 21, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 20, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 19, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 18, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 17, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 16, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 15, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 14, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 13, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 12, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 11, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 10, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 9, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 8, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 7, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 6, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 5, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 4, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 3, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 2, 
                    "Count": 0, 
                    "Converted": 0
                }, 
                {
                    "Score": 1, 
                    "Count": 0, 
                    "Converted": 0
                }
            ]
        }
    ]
        }
        
       '''
        obj = json.loads(jsonStr)
        return obj['Segmentations']

    def _loadModel1(self):
        jsonStr = '''
       {
    "Segmentations": [
        {
            "LeadSource": "All", 
            "Segments": [
                {
                    "Score": 100, 
                    "Count": 102, 
                    "Converted": 37
                }, 
                {
                    "Score": 99, 
                    "Count": 102, 
                    "Converted": 29
                }, 
                {
                    "Score": 98, 
                    "Count": 102, 
                    "Converted": 25
                }, 
                {
                    "Score": 97, 
                    "Count": 102, 
                    "Converted": 25
                }, 
                {
                    "Score": 96, 
                    "Count": 102, 
                    "Converted": 18
                }, 
                {
                    "Score": 95, 
                    "Count": 102, 
                    "Converted": 20
                }, 
                {
                    "Score": 94, 
                    "Count": 102, 
                    "Converted": 29
                }, 
                {
                    "Score": 93, 
                    "Count": 102, 
                    "Converted": 25
                }, 
                {
                    "Score": 92, 
                    "Count": 102, 
                    "Converted": 30
                }, 
                {
                    "Score": 91, 
                    "Count": 102, 
                    "Converted": 26
                }, 
                {
                    "Score": 90, 
                    "Count": 102, 
                    "Converted": 20
                }, 
                {
                    "Score": 89, 
                    "Count": 102, 
                    "Converted": 22
                }, 
                {
                    "Score": 88, 
                    "Count": 102, 
                    "Converted": 25
                }, 
                {
                    "Score": 87, 
                    "Count": 102, 
                    "Converted": 18
                }, 
                {
                    "Score": 86, 
                    "Count": 102, 
                    "Converted": 20
                }, 
                {
                    "Score": 85, 
                    "Count": 102, 
                    "Converted": 22
                }, 
                {
                    "Score": 84, 
                    "Count": 102, 
                    "Converted": 22
                }, 
                {
                    "Score": 83, 
                    "Count": 102, 
                    "Converted": 19
                }, 
                {
                    "Score": 82, 
                    "Count": 102, 
                    "Converted": 14
                }, 
                {
                    "Score": 81, 
                    "Count": 102, 
                    "Converted": 19
                }, 
                {
                    "Score": 80, 
                    "Count": 102, 
                    "Converted": 14
                }, 
                {
                    "Score": 79, 
                    "Count": 102, 
                    "Converted": 12
                }, 
                {
                    "Score": 78, 
                    "Count": 102, 
                    "Converted": 16
                }, 
                {
                    "Score": 77, 
                    "Count": 102, 
                    "Converted": 14
                }, 
                {
                    "Score": 76, 
                    "Count": 102, 
                    "Converted": 5
                }, 
                {
                    "Score": 75, 
                    "Count": 102, 
                    "Converted": 8
                }, 
                {
                    "Score": 74, 
                    "Count": 102, 
                    "Converted": 5
                }, 
                {
                    "Score": 73, 
                    "Count": 102, 
                    "Converted": 5
                }, 
                {
                    "Score": 72, 
                    "Count": 102, 
                    "Converted": 4
                }, 
                {
                    "Score": 71, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 70, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 69, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 68, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 67, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 66, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 65, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 64, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 63, 
                    "Count": 102, 
                    "Converted": 1
                }, 
                {
                    "Score": 62, 
                    "Count": 102, 
                    "Converted": 1
                }, 
                {
                    "Score": 61, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 60, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 59, 
                    "Count": 102, 
                    "Converted": 1
                }, 
                {
                    "Score": 58, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 57, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 56, 
                    "Count": 102, 
                    "Converted": 4
                }, 
                {
                    "Score": 55, 
                    "Count": 102, 
                    "Converted": 4
                }, 
                {
                    "Score": 54, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 53, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 52, 
                    "Count": 102, 
                    "Converted": 3
                }, 
                {
                    "Score": 51, 
                    "Count": 102, 
                    "Converted": 1
                }, 
                {
                    "Score": 50, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 49, 
                    "Count": 102, 
                    "Converted": 1
                }, 
                {
                    "Score": 48, 
                    "Count": 102, 
                    "Converted": 2
                }, 
                {
                    "Score": 47, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 46, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 45, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 44, 
                    "Count": 102, 
                    "Converted": 1
                }, 
                {
                    "Score": 43, 
                    "Count": 102, 
                    "Converted": 0
                }, 
                {
                    "Score": 42, 
                    "Count": 102, 
                    "Converted": 1
                }, 
                {
                    "Score": 41, 
                    "Count": 101, 
                    "Converted": 2
                }, 
                {
                    "Score": 40, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 39, 
                    "Count": 101, 
                    "Converted": 2
                }, 
                {
                    "Score": 38, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 37, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 36, 
                    "Count": 101, 
                    "Converted": 2
                }, 
                {
                    "Score": 35, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 34, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 33, 
                    "Count": 101, 
                    "Converted": 1
                }, 
                {
                    "Score": 32, 
                    "Count": 101, 
                    "Converted": 1
                }, 
                {
                    "Score": 31, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 30, 
                    "Count": 101, 
                    "Converted": 2
                }, 
                {
                    "Score": 29, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 28, 
                    "Count": 101, 
                    "Converted": 1
                }, 
                {
                    "Score": 27, 
                    "Count": 101, 
                    "Converted": 1
                }, 
                {
                    "Score": 26, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 25, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 24, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 23, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 22, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 21, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 20, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 19, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 18, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 17, 
                    "Count": 101, 
                    "Converted": 1
                }, 
                {
                    "Score": 16, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 15, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 14, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 13, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 12, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 11, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 10, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 9, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 8, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 7, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 6, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 5, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 4, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 3, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 2, 
                    "Count": 101, 
                    "Converted": 0
                }, 
                {
                    "Score": 1, 
                    "Count": 101, 
                    "Converted": 0
                }
            ]
        }
    ] 
}
       '''
        obj = json.loads(jsonStr)
        return obj['Segmentations']

    def _loadEVModel(self):
        jsonStr = '''
        {
    "EVSegmentations": 
[
 {
  "LeadSource": "ALL", 
  "Segments": [
   {
    "Score": 100, 
    "Count": 571.0, 
    "Sum": 1004132.3617335001, 
    "Mean": 1758.5505459430826, 
    "Max": 2513.9163148997914, 
    "Min": 1141.6331579876835
   }, 
   {
    "Score": 99, 
    "Count": 571.0, 
    "Sum": 519299.4350268073, 
    "Mean": 909.4561033744436, 
    "Max": 1141.6331579876835, 
    "Min": 732.9144677652313
   }, 
   {
    "Score": 98, 
    "Count": 571.0, 
    "Sum": 362126.1828534892, 
    "Mean": 634.1964673441141, 
    "Max": 732.9144677652313, 
    "Min": 549.9670579041111
   }, 
   {
    "Score": 97, 
    "Count": 571.0, 
    "Sum": 282291.84474518185, 
    "Mean": 494.3815144398982, 
    "Max": 549.9670579041111, 
    "Min": 447.335751438029
   }, 
   {
    "Score": 96, 
    "Count": 571.0, 
    "Sum": 233434.85092458397, 
    "Mean": 408.81760231976176, 
    "Max": 447.335751438029, 
    "Min": 375.9429790902847
   }, 
   {
    "Score": 95, 
    "Count": 570.0, 
    "Sum": 196966.2102374558, 
    "Mean": 345.554754802554, 
    "Max": 375.9429790902847, 
    "Min": 317.0841273340582
   }, 
   {
    "Score": 94, 
    "Count": 571.0, 
    "Sum": 167897.0323368813, 
    "Mean": 294.0403368421739, 
    "Max": 317.0841273340582, 
    "Min": 273.7688290598078
   }, 
   {
    "Score": 93, 
    "Count": 571.0, 
    "Sum": 146795.89175237043, 
    "Mean": 257.0856247852372, 
    "Max": 273.7688290598078, 
    "Min": 240.43495254769067
   }, 
   {
    "Score": 92, 
    "Count": 571.0, 
    "Sum": 129191.77518546498, 
    "Mean": 226.2552980480998, 
    "Max": 240.43495254769067, 
    "Min": 212.66197161383775
   }, 
   {
    "Score": 91, 
    "Count": 571.0, 
    "Sum": 114437.80922589697, 
    "Mean": 200.41647850419784, 
    "Max": 212.66197161383775, 
    "Min": 190.09069044257876
   }, 
   {
    "Score": 90, 
    "Count": 571.0, 
    "Sum": 103411.31278774745, 
    "Mean": 181.10562659850692, 
    "Max": 190.09069044257876, 
    "Min": 172.8792868176404
   }, 
   {
    "Score": 89, 
    "Count": 570.0, 
    "Sum": 93780.57856233984, 
    "Mean": 164.52733081112254, 
    "Max": 172.8792868176404, 
    "Min": 156.8112259636339
   }, 
   {
    "Score": 88, 
    "Count": 571.0, 
    "Sum": 85659.34231279849, 
    "Mean": 150.01636131838615, 
    "Max": 156.8112259636339, 
    "Min": 144.0378862434965
   }, 
   {
    "Score": 87, 
    "Count": 571.0, 
    "Sum": 78662.99956497765, 
    "Mean": 137.76357191764913, 
    "Max": 144.0378862434965, 
    "Min": 132.10721442563246
   }, 
   {
    "Score": 86, 
    "Count": 571.0, 
    "Sum": 72598.95910472068, 
    "Mean": 127.14353608532518, 
    "Max": 132.00703562117448, 
    "Min": 122.27564285532041
   }, 
   {
    "Score": 85, 
    "Count": 571.0, 
    "Sum": 67147.17075172346, 
    "Mean": 117.5957456247346, 
    "Max": 122.27564285532041, 
    "Min": 113.03065447216937
   }, 
   {
    "Score": 84, 
    "Count": 571.0, 
    "Sum": 62312.67150620398, 
    "Mean": 109.12902190228368, 
    "Max": 113.03065447216937, 
    "Min": 105.18644172170157
   }, 
   {
    "Score": 83, 
    "Count": 570.0, 
    "Sum": 57956.25523704023, 
    "Mean": 101.67764076673726, 
    "Max": 105.18644172170157, 
    "Min": 98.21905875563127
   }, 
   {
    "Score": 82, 
    "Count": 571.0, 
    "Sum": 54361.074248869525, 
    "Mean": 95.20328239731965, 
    "Max": 98.21905875563127, 
    "Min": 92.18621021005428
   }, 
   {
    "Score": 81, 
    "Count": 571.0, 
    "Sum": 51028.22491233844, 
    "Mean": 89.36641841040007, 
    "Max": 92.18621021005428, 
    "Min": 86.40583402016829
   }, 
   {
    "Score": 80, 
    "Count": 571.0, 
    "Sum": 47823.98850465582, 
    "Mean": 83.75479598013278, 
    "Max": 86.40583402016829, 
    "Min": 81.27208036351546
   }, 
   {
    "Score": 79, 
    "Count": 571.0, 
    "Sum": 45025.0224237487, 
    "Mean": 78.85292893826391, 
    "Max": 81.27208036351546, 
    "Min": 76.61444225196114
   }, 
   {
    "Score": 78, 
    "Count": 571.0, 
    "Sum": 42407.012364140406, 
    "Mean": 74.26797261670824, 
    "Max": 76.61444225196114, 
    "Min": 72.14708009169016
   }, 
   {
    "Score": 77, 
    "Count": 570.0, 
    "Sum": 39977.832222147415, 
    "Mean": 70.13654775815336, 
    "Max": 72.14708009169016, 
    "Min": 68.09700810430647
   }, 
   {
    "Score": 76, 
    "Count": 571.0, 
    "Sum": 37749.43340978018, 
    "Mean": 66.11109178595477, 
    "Max": 68.09700810430647, 
    "Min": 64.19473539451002
   }, 
   {
    "Score": 75, 
    "Count": 571.0, 
    "Sum": 35721.31947820149, 
    "Mean": 62.55922850823379, 
    "Max": 64.19473539451002, 
    "Min": 60.87342304172406
   }, 
   {
    "Score": 74, 
    "Count": 571.0, 
    "Sum": 33748.965768286886, 
    "Mean": 59.105018858646034, 
    "Max": 60.87342304172406, 
    "Min": 57.40316182027105
   }, 
   {
    "Score": 73, 
    "Count": 571.0, 
    "Sum": 31991.057699428417, 
    "Mean": 56.02637075206378, 
    "Max": 57.40316182027105, 
    "Min": 54.40332269085033
   }, 
   {
    "Score": 72, 
    "Count": 571.0, 
    "Sum": 30237.56802393256, 
    "Mean": 52.95546063736, 
    "Max": 54.40332269085033, 
    "Min": 51.41214995716704
   }, 
   {
    "Score": 71, 
    "Count": 570.0, 
    "Sum": 28511.13938672505, 
    "Mean": 50.01954278372816, 
    "Max": 51.41214995716704, 
    "Min": 48.70106656611947
   }, 
   {
    "Score": 70, 
    "Count": 571.0, 
    "Sum": 27058.375034501496, 
    "Mean": 47.38769708319001, 
    "Max": 48.70106656611947, 
    "Min": 46.01515989789588
   }, 
   {
    "Score": 69, 
    "Count": 571.0, 
    "Sum": 25553.8033766394, 
    "Mean": 44.75272044945604, 
    "Max": 46.01515989789588, 
    "Min": 43.32392795410524
   }, 
   {
    "Score": 68, 
    "Count": 571.0, 
    "Sum": 24031.88549131145, 
    "Mean": 42.08736513364527, 
    "Max": 43.32392795410524, 
    "Min": 40.84051993875288
   }, 
   {
    "Score": 67, 
    "Count": 571.0, 
    "Sum": 22632.69246594631, 
    "Mean": 39.63693952004608, 
    "Max": 40.83200907373684, 
    "Min": 38.40020869785624
   }, 
   {
    "Score": 66, 
    "Count": 571.0, 
    "Sum": 21210.007578009743, 
    "Mean": 37.145372290735104, 
    "Max": 38.40020869785624, 
    "Min": 35.82419215274872
   }, 
   {
    "Score": 65, 
    "Count": 570.0, 
    "Sum": 19754.848812658376, 
    "Mean": 34.65762949589189, 
    "Max": 35.82908587427671, 
    "Min": 33.46972672402623
   }, 
   {
    "Score": 64, 
    "Count": 571.0, 
    "Sum": 18479.77542632375, 
    "Mean": 32.363879905996065, 
    "Max": 33.46972672402623, 
    "Min": 31.310027319757264
   }, 
   {
    "Score": 63, 
    "Count": 571.0, 
    "Sum": 17314.279427347283, 
    "Mean": 30.32273104614235, 
    "Max": 31.310027319757264, 
    "Min": 29.408650019009073
   }, 
   {
    "Score": 62, 
    "Count": 571.0, 
    "Sum": 16289.217326026215, 
    "Mean": 28.52752596501964, 
    "Max": 29.408650019009073, 
    "Min": 27.65187105845617
   }, 
   {
    "Score": 61, 
    "Count": 571.0, 
    "Sum": 15279.968103883204, 
    "Mean": 26.760014192439936, 
    "Max": 27.65187105845617, 
    "Min": 25.8643099893536
   }, 
   {
    "Score": 60, 
    "Count": 571.0, 
    "Sum": 14288.75345727829, 
    "Mean": 25.024086615198406, 
    "Max": 25.838680777597272, 
    "Min": 24.288697979912538
   }, 
   {
    "Score": 59, 
    "Count": 570.0, 
    "Sum": 13419.338899531755, 
    "Mean": 23.54269982373992, 
    "Max": 24.288697979912538, 
    "Min": 22.816225180205247
   }, 
   {
    "Score": 58, 
    "Count": 571.0, 
    "Sum": 12641.954137087236, 
    "Mean": 22.140024758471515, 
    "Max": 22.816225180205247, 
    "Min": 21.448634959430454
   }, 
   {
    "Score": 57, 
    "Count": 571.0, 
    "Sum": 11919.70376756903, 
    "Mean": 20.87513794670583, 
    "Max": 21.45044890438168, 
    "Min": 20.268470379464787
   }, 
   {
    "Score": 56, 
    "Count": 571.0, 
    "Sum": 11238.535415996574, 
    "Mean": 19.682198626964226, 
    "Max": 20.268470379464787, 
    "Min": 19.137916555483212
   }, 
   {
    "Score": 55, 
    "Count": 571.0, 
    "Sum": 10581.459957527597, 
    "Mean": 18.531453515810153, 
    "Max": 19.137916555483212, 
    "Min": 18.02043612263674
   }, 
   {
    "Score": 54, 
    "Count": 571.0, 
    "Sum": 9996.426688287838, 
    "Mean": 17.506876862150328, 
    "Max": 18.02312441677209, 
    "Min": 16.97343687253865
   }, 
   {
    "Score": 53, 
    "Count": 570.0, 
    "Sum": 9413.993826016118, 
    "Mean": 16.51577864213354, 
    "Max": 16.97343687253865, 
    "Min": 16.051352492652804
   }, 
   {
    "Score": 52, 
    "Count": 571.0, 
    "Sum": 8910.629752423707, 
    "Mean": 15.605306046276194, 
    "Max": 16.0538439975184, 
    "Min": 15.166802920422535
   }, 
   {
    "Score": 51, 
    "Count": 571.0, 
    "Sum": 8418.442001703701, 
    "Mean": 14.7433310012324, 
    "Max": 15.15953861778742, 
    "Min": 14.325113089229395
   }, 
   {
    "Score": 50, 
    "Count": 571.0, 
    "Sum": 7940.579528389979, 
    "Mean": 13.90644400768823, 
    "Max": 14.325113089229395, 
    "Min": 13.520953874954083
   }, 
   {
    "Score": 49, 
    "Count": 571.0, 
    "Sum": 7518.053704042684, 
    "Mean": 13.166468833699973, 
    "Max": 13.520953874954083, 
    "Min": 12.820410878523223
   }, 
   {
    "Score": 48, 
    "Count": 570.0, 
    "Sum": 7119.158698468623, 
    "Mean": 12.489752102576531, 
    "Max": 12.820410878523223, 
    "Min": 12.154547770288442
   }, 
   {
    "Score": 47, 
    "Count": 571.0, 
    "Sum": 6760.857258338343, 
    "Mean": 11.840380487457693, 
    "Max": 12.13956251795341, 
    "Min": 11.540083860602348
   }, 
   {
    "Score": 46, 
    "Count": 571.0, 
    "Sum": 6406.976000329388, 
    "Mean": 11.220623468177562, 
    "Max": 11.540083860602348, 
    "Min": 10.907506614843356
   }, 
   {
    "Score": 45, 
    "Count": 571.0, 
    "Sum": 6074.467452992542, 
    "Mean": 10.638296765310931, 
    "Max": 10.907506614843356, 
    "Min": 10.36845084154035
   }, 
   {
    "Score": 44, 
    "Count": 571.0, 
    "Sum": 5763.91204737843, 
    "Mean": 10.09441689558394, 
    "Max": 10.370184371609566, 
    "Min": 9.835435405107146
   }, 
   {
    "Score": 43, 
    "Count": 571.0, 
    "Sum": 5481.309594462196, 
    "Mean": 9.599491408865493, 
    "Max": 9.835435405107146, 
    "Min": 9.340524927995679
   }, 
   {
    "Score": 42, 
    "Count": 570.0, 
    "Sum": 5197.903857356325, 
    "Mean": 9.119129574309342, 
    "Max": 9.345210748577788, 
    "Min": 8.88695536955781
   }, 
   {
    "Score": 41, 
    "Count": 571.0, 
    "Sum": 4952.67551539885, 
    "Mean": 8.67368741751112, 
    "Max": 8.88695536955781, 
    "Min": 8.46496692637069
   }, 
   {
    "Score": 40, 
    "Count": 571.0, 
    "Sum": 4722.9338171626005, 
    "Mean": 8.271337683297023, 
    "Max": 8.45334536051124, 
    "Min": 8.087291517320901
   }, 
   {
    "Score": 39, 
    "Count": 571.0, 
    "Sum": 4531.73361466208, 
    "Mean": 7.936486190301366, 
    "Max": 8.08908657154459, 
    "Min": 7.774189626743661
   }, 
   {
    "Score": 38, 
    "Count": 571.0, 
    "Sum": 4353.951021710277, 
    "Mean": 7.62513313784637, 
    "Max": 7.774189626743661, 
    "Min": 7.475102091089056
   }, 
   {
    "Score": 37, 
    "Count": 571.0, 
    "Sum": 4174.6753763096385, 
    "Mean": 7.311165282503746, 
    "Max": 7.475102091089056, 
    "Min": 7.139836151572996
   }, 
   {
    "Score": 36, 
    "Count": 570.0, 
    "Sum": 3971.48097265613, 
    "Mean": 6.967510478344088, 
    "Max": 7.142228284572412, 
    "Min": 6.804636622500592
   }, 
   {
    "Score": 35, 
    "Count": 571.0, 
    "Sum": 3792.211289544565, 
    "Mean": 6.641350769780324, 
    "Max": 6.8102637795564664, 
    "Min": 6.492426341908714
   }, 
   {
    "Score": 34, 
    "Count": 571.0, 
    "Sum": 3613.8960771721795, 
    "Mean": 6.329064933751628, 
    "Max": 6.495812518640685, 
    "Min": 6.197977974526265
   }, 
   {
    "Score": 33, 
    "Count": 571.0, 
    "Sum": 3452.6973579206096, 
    "Mean": 6.046755442943274, 
    "Max": 6.200425406673149, 
    "Min": 5.9110461484176895
   }, 
   {
    "Score": 32, 
    "Count": 571.0, 
    "Sum": 3294.163085672338, 
    "Mean": 5.769112234102169, 
    "Max": 5.914106876287502, 
    "Min": 5.6371865117274504
   }, 
   {
    "Score": 31, 
    "Count": 571.0, 
    "Sum": 3129.7154544996747, 
    "Mean": 5.481112880034456, 
    "Max": 5.6371865117274504, 
    "Min": 5.335744740162033
   }, 
   {
    "Score": 30, 
    "Count": 570.0, 
    "Sum": 2967.818866200324, 
    "Mean": 5.206699765263727, 
    "Max": 5.339548346428515, 
    "Min": 5.089648836672243
   }, 
   {
    "Score": 29, 
    "Count": 571.0, 
    "Sum": 2837.025322675113, 
    "Mean": 4.968520705210356, 
    "Max": 5.095971784614727, 
    "Min": 4.8350480530631765
   }, 
   {
    "Score": 28, 
    "Count": 571.0, 
    "Sum": 2697.8749034722605, 
    "Mean": 4.7248246996011565, 
    "Max": 4.836271933148232, 
    "Min": 4.616275208272291
   }, 
   {
    "Score": 27, 
    "Count": 571.0, 
    "Sum": 2582.4420275475854, 
    "Mean": 4.522665547368801, 
    "Max": 4.619408614134109, 
    "Min": 4.435939520757709
   }, 
   {
    "Score": 26, 
    "Count": 571.0, 
    "Sum": 2468.530017922557, 
    "Mean": 4.323169908796071, 
    "Max": 4.438930750956688, 
    "Min": 4.237999518203192
   }, 
   {
    "Score": 25, 
    "Count": 571.0, 
    "Sum": 2361.668904575461, 
    "Mean": 4.136022599957025, 
    "Max": 4.242245577389535, 
    "Min": 4.028811339746775
   }, 
   {
    "Score": 24, 
    "Count": 570.0, 
    "Sum": 2246.5154102125525, 
    "Mean": 3.941255105636057, 
    "Max": 4.033801491482985, 
    "Min": 3.8368368359602476
   }, 
   {
    "Score": 23, 
    "Count": 571.0, 
    "Sum": 2149.0900501472656, 
    "Mean": 3.763730385546875, 
    "Max": 3.8412377498076067, 
    "Min": 3.667512681555462
   }, 
   {
    "Score": 22, 
    "Count": 571.0, 
    "Sum": 2050.2416171918585, 
    "Mean": 3.5906157919296997, 
    "Max": 3.670632935137747, 
    "Min": 3.500701018434273
   }, 
   {
    "Score": 21, 
    "Count": 571.0, 
    "Sum": 1949.9604796403803, 
    "Mean": 3.4149920834332406, 
    "Max": 3.5069149534577733, 
    "Min": 3.3298878294477046
   }, 
   {
    "Score": 20, 
    "Count": 571.0, 
    "Sum": 1861.4555121528078, 
    "Mean": 3.259992140372693, 
    "Max": 3.3324076347134675, 
    "Min": 3.1986477168549894
   }, 
   {
    "Score": 19, 
    "Count": 571.0, 
    "Sum": 1785.7911234383898, 
    "Mean": 3.127480076074238, 
    "Max": 3.1986477168549894, 
    "Min": 3.06555895601136
   }, 
   {
    "Score": 18, 
    "Count": 570.0, 
    "Sum": 1721.444316328709, 
    "Mean": 3.0200777479451033, 
    "Max": 3.068127906599604, 
    "Min": 2.9759017156100414
   }, 
   {
    "Score": 17, 
    "Count": 571.0, 
    "Sum": 1669.5438880346503, 
    "Mean": 2.9238947251044665, 
    "Max": 2.97900700953568, 
    "Min": 2.8664151592693665
   }, 
   {
    "Score": 16, 
    "Count": 571.0, 
    "Sum": 1600.7273461554882, 
    "Mean": 2.803375387312589, 
    "Max": 2.868218049124279, 
    "Min": 2.7474495765930986
   }, 
   {
    "Score": 15, 
    "Count": 571.0, 
    "Sum": 1536.8222580019265, 
    "Mean": 2.691457544661868, 
    "Max": 2.748200939616569, 
    "Min": 2.629584233416022
   }, 
   {
    "Score": 14, 
    "Count": 571.0, 
    "Sum": 1475.2928512764815, 
    "Mean": 2.583700264932542, 
    "Max": 2.6328836765096413, 
    "Min": 2.5359873000398867
   }, 
   {
    "Score": 13, 
    "Count": 571.0, 
    "Sum": 1431.3281369706576, 
    "Mean": 2.5067042678995755, 
    "Max": 2.54156666831216, 
    "Min": 2.4727983691059654
   }, 
   {
    "Score": 12, 
    "Count": 570.0, 
    "Sum": 1396.4891410721214, 
    "Mean": 2.4499809492493356, 
    "Max": 2.4787044253208186, 
    "Min": 2.4262138122262527
   }, 
   {
    "Score": 11, 
    "Count": 571.0, 
    "Sum": 1369.8996793849983, 
    "Mean": 2.3991237817600672, 
    "Max": 2.433766024361508, 
    "Min": 2.362626765707171
   }, 
   {
    "Score": 10, 
    "Count": 571.0, 
    "Sum": 1325.417275770984, 
    "Mean": 2.3212211484605674, 
    "Max": 2.368178360828488, 
    "Min": 2.280902847949362
   }, 
   {
    "Score": 9, 
    "Count": 571.0, 
    "Sum": 1284.768187884743, 
    "Mean": 2.250031852687816, 
    "Max": 2.2824608294182718, 
    "Min": 2.2201555569118656
   }, 
   {
    "Score": 8, 
    "Count": 571.0, 
    "Sum": 1246.4096323597234, 
    "Mean": 2.1828539971273617, 
    "Max": 2.2236398967659126, 
    "Min": 2.135294361526395
   }, 
   {
    "Score": 7, 
    "Count": 571.0, 
    "Sum": 1189.8641690728714, 
    "Mean": 2.0838251647510884, 
    "Max": 2.139504583135927, 
    "Min": 2.049443505748938
   }, 
   {
    "Score": 6, 
    "Count": 570.0, 
    "Sum": 1146.324535500308, 
    "Mean": 2.0110956763163297, 
    "Max": 2.054086906030163, 
    "Min": 1.9760885538445367
   }, 
   {
    "Score": 5, 
    "Count": 571.0, 
    "Sum": 1113.577843345017, 
    "Mean": 1.9502238937741105, 
    "Max": 1.9807864312507222, 
    "Min": 1.9241313450236783
   }, 
   {
    "Score": 4, 
    "Count": 571.0, 
    "Sum": 1065.8313029401081, 
    "Mean": 1.8666047336954608, 
    "Max": 1.927730264876832, 
    "Min": 1.7966933067103796
   }, 
   {
    "Score": 3, 
    "Count": 571.0, 
    "Sum": 989.8949902083648, 
    "Mean": 1.7336164451985372, 
    "Max": 1.800166796082427, 
    "Min": 1.6687929113905942
   }, 
   {
    "Score": 2, 
    "Count": 571.0, 
    "Sum": 932.9720703020216, 
    "Mean": 1.6339265679545036, 
    "Max": 1.6744324119841714, 
    "Min": 1.6041850986983937
   }, 
   {
    "Score": 1, 
    "Count": 570.0, 
    "Sum": 888.7428111836275, 
    "Mean": 1.5591979143572412, 
    "Max": 1.6041850986983937, 
    "Min": 1.45261575230471
   }
  ]
 }
]
}
    '''
        obj = json.loads(jsonStr)
        return obj['EVSegmentations']
