from numpy import inf

from sklearn import metrics
from sklearn.metrics.cluster.supervised import entropy

import pandas as pd
from profilingtestbase import ProfilingTestBase

class TopPredictorsTest(ProfilingTestBase):

    def tearDown(self):
        super(ProfilingTestBase, self).tearDown()
        self.tearDownClass()
        self.setUpClass()

    def testTopPredictor(self):
        data = range(10, 100, 10)
        # This gives [10, 20, 30, 40, 50, 60, 70, 80, 90]
        event = [1, 1, 0, 1, 0, 1, 0, 0, 1]
        print(len(data), len(event))
        buckets = map(classify_input, data)
        
        execfile("data_profile.py", globals())
        mi, component_mi = globals()["calculateMutualInfo"](buckets, event)
        
        expectedMi = metrics.mutual_info_score(buckets, event)
        self.assertEqual(round(mi, 4), round(expectedMi, 4)) 
        self.assertEqual(round(mi / entropy(event), 4), 0.0734)
        
        self.assertEqual(round(component_mi['small'] / entropy(event), 4), 0.0124)
        self.assertEqual(round(component_mi['large'] / entropy(event), 4), 0.0485)
        self.assertEqual(round(component_mi['medium'] / entropy(event), 4), 0.0124)
        
        component_uc = {k:v/entropy(event) for k, v in component_mi.iteritems()}

        overallUc = expectedMi/entropy(event)
        sumOfUc = sum(component_uc.values())
        print('Overall UC: ', overallUc)
        print('Component UC: ', component_uc)
        print('Summed up component UC: ', sumOfUc)
        
        self.assertTrue(abs(overallUc - sumOfUc) < 0.0001)

    def testTopPredictorWithNone(self):
        columnVector = pd.Series([-1, 1, 3, 5, None, 6, 8])
        bands = [-inf, 0, 2, 8, 10]
        
        execfile("data_profile.py", globals())
        result = globals()["mapToBands"](columnVector, bands)
        self.assertEqual(result, [-inf, 0, 2, 2, None, 2, 8]) 
        event = [0, 1, 0, 1, 0, 0, 0]
        mi, component_mi = globals()["calculateMutualInfo"](result, event)
        expectedMi = metrics.mutual_info_score(result, event)
        self.assertEqual (round(mi, 4), 0.3255)
        self.assertEqual(len(component_mi), 5)
        self.assertEqual(round(component_mi[0], 4), 0.179)
        self.assertEqual(round(component_mi[2], 4), 0.0023)
        self.assertEqual(round(component_mi[8], 4), 0.0481)
        self.assertEqual(round(component_mi[None], 4), 0.0481)
        self.assertEqual(round(component_mi[-inf], 4), 0.0481)

        component_uc = {k:v/entropy(event) for k, v in component_mi.iteritems()}
        overallUc = expectedMi/entropy(event)
        sumOfUc = sum(component_uc.values())
        print('Overall UC: ', overallUc)
        print('Component UC: ', component_uc)
        print('Summed up component UC: ', sumOfUc)
        self.assertTrue(abs(overallUc - sumOfUc) < 0.0001)

    
    def testCalculateMutualInfoAllZeroEvents(self):
        data = range(10, 100, 10)
        # This gives [10, 20, 30, 40, 50, 60, 70, 80, 90]
        event = [0, 0, 0, 0, 0, 0, 0, 0, 0]
        print(len(data), len(event))
        buckets = map(classify_input, data)
        
        execfile("data_profile.py", globals())
        mi, component_mi = globals()["calculateMutualInfo"](buckets, event)
        self.assertEqual(mi, 0)
        y = entropy(event)
        for _, v in component_mi.iteritems():
            self.assertEqual(v, 0)
            self.assertEqual(y, 0)
            uc = globals()["uncertaintyCoefficient"](v, y)
            self.assertEqual(uc, None)
            
        
        uc = globals()["uncertaintyCoefficient"](None, 0.5)
        self.assertEqual(uc, None)

def classify_input(x):
    classification = 'unknown'
    if x > 0 and x <= 30:
        classification = 'small'
    elif x > 30 and x <= 60:
        classification = 'medium'
    elif x > 60 and x <= 90:
        classification = 'large'
    return classification
    

