import json
from testbase import TestBase
from leframework.model.states.generatefitfunction import FitFunctionGenerator
from leframework.model.mediator import Mediator


class FitFunctionGeneratorTest(TestBase):

    def setUp(self):
        self.fit = FitFunctionGenerator()
        mediator = Mediator()
        mediator.segmentations = self._loadSegmentations()
        self.fit.mediator = mediator

    def testExecute(self):
        self.assertEquals(self.fit.getName(), "FitFunctionGenerator")
        self.fit.execute()
        fit_function_parameters = self.fit.getMediator().fit_function_parameters
        print(fit_function_parameters)
        # validate alpha, beta, gamma, maxRate, they are
        # -63472.8123838 730757.154341 100000 0.544117647059
        self.assertAlmostEquals(fit_function_parameters['alpha'], -63472.8123838, delta=1e-6)
        self.assertAlmostEquals(fit_function_parameters['beta'], 730757.154341, delta=1e-6)
        self.assertAlmostEquals(fit_function_parameters['gamma'], 100000, delta=1e-6)
        self.assertAlmostEquals(fit_function_parameters['maxRate'], 0.544117647059, delta=1e-6)

    def _loadSegmentations(self):
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
