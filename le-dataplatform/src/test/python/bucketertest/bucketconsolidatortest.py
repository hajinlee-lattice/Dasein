
import pandas as pd
import numpy as np
from leframework.bucketers.bucketconsolidator import BucketConsolidator
from bucketertestbase import BucketerTestBase
from testbase import TestBase


class BucketerConsolidatorTest(TestBase, BucketerTestBase):

    def setUp(self):
        self.bucketer = BucketConsolidator(5, 0.4, 0.2)

    def testBucketConsolidator(self):
        foo = pd.DataFrame(columns=['x', 'event'])
        foo['x'] = np.random.normal(50, 20, 10000)
        foo['x'] = np.random.uniform(0, 1500, 10000)
        foo['event'] = np.random.choice(2, 10000, p=[0.95, 0.05])
        foo[foo['x'] < 0] = 0

        oldBins = [0, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000]
        resultBins = self.bucketer.consolidateBins(foo['x'], foo['event'], oldBins)

        self.assertEqual(resultBins[0], 0)
        self.assertEqual(resultBins[-1], 1000)
        self.assertTrue(len(resultBins) <= 5)
        self.assertTrue(len(resultBins) >= 2)
        self.assertTrue(len(resultBins) < len(oldBins))

    def testGetNextConsolidationByLift_0_3(self):
        zippedList = [(1, 5), (5, 10), (10, 20), (20, 50), (50, 100), (100, 200)]

        eventSums = [(2, 0), (2, 10), (5, 10), (6, 10), (6, 10), (10, 10)]
        removedIndex = self.bucketer._getNextConsolidationByLift(zippedList, eventSums)
        self.assertEqual(removedIndex, 0)

        eventSums = [(2, 10), (2, 10), (5, 10), (6, 10), (6, 10), (10, 10)]
        removedIndex = self.bucketer._getNextConsolidationByLift(zippedList, eventSums)
        self.assertEqual(removedIndex, 0)

        eventSums = [(2, 10), (3, 10), (5, 10), (6, 10), (6, 10), (10, 10)]
        removedIndex = self.bucketer._getNextConsolidationByLift(zippedList, eventSums)
        self.assertEqual(removedIndex, 3)

        eventSums = [(1, 10), (2, 10), (3, 10), (4, 10), (5, 10), (6, 10)]
        removedIndex = self.bucketer._getNextConsolidationByLift(zippedList, eventSums)
        self.assertEqual(removedIndex, 3)

        eventSums = [(1, 100), (2, 100), (3, 100), (4, 100), (5, 100), (6, 100)]
        removedIndex = self.bucketer._getNextConsolidationByLift(zippedList, eventSums)
        self.assertEqual(removedIndex, 4)

    def testGetNextConsolidationByLift_0_2(self):

        zippedList = [(1, 5), (5, 10), (10, 20), (20, 50), (50, 100), (100, 200)]
        bucketer2 = BucketConsolidator(5, 0.2, 0.2)

        eventSums = [(1, 10), (2, 10), (3, 10), (4, 10), (5, 10), (6, 10)]
        removedIndex = bucketer2._getNextConsolidationByLift(zippedList, eventSums)
        self.assertEqual(removedIndex, 3)

        eventSums = [(1, 100), (2, 100), (3, 100), (4, 100), (5, 100), (6, 100)]
        removedIndex = bucketer2._getNextConsolidationByLift(zippedList, eventSums)
        self.assertEqual(removedIndex, 4)

        zippedList = [(1, 5), (5, 10), (10, 20)]
        eventSums = [(1, 2), (2, 3), (3, 4)]
        removedIndex = bucketer2._getNextConsolidationByLift(zippedList, eventSums)
        self.assertEqual(removedIndex, 1)

    def testCombineAdjacentBins(self):
        zippedList = [(1, 5), (5, 10), (10, 20), (20, 50), (50, 100), (100, 200)]
        eventSums = [(2, 10), (3, 10), (5, 10), (6, 10), (6, 10), (10, 10)]
        newZippedList, newEventSums = self.bucketer._combineAdjacentBins(zippedList, eventSums, 2);
        self.assertEqual(newZippedList, [(1, 5), (5, 10), (10, 50), (50, 100), (100, 200)])
        self.assertEqual(newEventSums, [(2, 10), (3, 10), (11, 20), (6, 10), (10, 10)])
