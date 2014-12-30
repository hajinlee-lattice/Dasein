import numpy as np
import scipy.stats

class BucketConsolidator(object):

    def __init__(self, maxBuckets, liftThreshold, ratioTreshold):
        self.maxBuckets = maxBuckets
        self.liftThreshold = liftThreshold
        self.ratioThreshold = ratioTreshold
    
    def _getNextConsolidationByLift(self, zippedList, eventSums):
        """ 
        Finds the the smallest pair of adjacent bins that have a lift difference
        less than liftThreshold.
        
        Returns the index of the first bin 
        """
        if eventSums[0][1] == 0:
            return 0

        removeIndex = None
        tempRemoveValue = np.inf
        averageProbability = float(sum((x[0] for x in eventSums))) / sum(x[1] for x in eventSums)
                    
        for i in range(len(zippedList) - 1):
            if eventSums[i + 1][1] == 0:
                removeIndex = i
                break
            
            firstConversion = eventSums[i][0] / float(eventSums[i][1])
            secondConversion = eventSums[i + 1][0] / float(eventSums[i + 1][1])
            firstLift = firstConversion / averageProbability
            secondLift = secondConversion / averageProbability

            # if two adjacent buckets have the exact same lift always combine them
            if firstLift == secondLift:
                removeIndex = i
                break

            if (firstLift > 1.0 and secondLift > 1.0): 
                # use ratio
                delta = abs(1.0 - firstLift / secondLift)
                if delta < self.ratioThreshold:
                    removeIndex = i
                    break
            else:
                # use substraction
                delta = abs(firstLift - secondLift)
                if delta < tempRemoveValue and delta <= self.liftThreshold:
                    tempRemoveValue = delta
                    removeIndex = i
        return removeIndex

    @staticmethod
    def _combineAdjacentBins(zippedList, eventSums, removeIndex):
        """ Helper method to combine the bins adjacent to the removed index. """
        zippedList[removeIndex] = (zippedList[removeIndex][0], zippedList[removeIndex + 1][1])
        eventSums[removeIndex] = (eventSums[removeIndex][0] + eventSums[removeIndex + 1][0], eventSums[removeIndex][1] + eventSums[removeIndex + 1][1])
        zippedList.remove(zippedList[removeIndex + 1])
        eventSums.remove(eventSums[removeIndex + 1])
        return (zippedList, eventSums)

    def consolidateBins(self, columnSeries, eventSeries, bucketList):
        """
        Method to consolidate a list of continuous attribute buckets given an attribute column and event column.  Ensures that the bucket count is 
        less than maxBuckets by removing adjacent ranges that have the maximum probability of resulting from a binomial distribution of the combined 
        ranges
        """
        if self.maxBuckets <= 0:
            raise ValueError("maxBuckets cannot be less than or equal to zero.")

        zippedList = zip(bucketList, bucketList[1::])
        getSelector = lambda x, nextX: (columnSeries > x) & (columnSeries <= nextX)
        eventSums = [(eventSeries[getSelector(x, nextX)].sum(), np.count_nonzero(getSelector(x, nextX))) for x, nextX in zippedList]

        # first pass, get the number of buckets down below maxBuckets
        while (len(zippedList) > self.maxBuckets):
            maxDelta = 0
            removeIndex = 0
            for i in range(len(zippedList) - 1):
                newConversionRate = float(eventSums[i][0] + eventSums[i + 1][0] + 1) / float(eventSums[i][1] + eventSums[i + 1][1] + 1)
                delta = scipy.stats.binom.pmf(eventSums[i][0], eventSums[i][1], newConversionRate) * scipy.stats.binom.pmf(eventSums[i + 1][0], eventSums[i + 1][1], newConversionRate)
                if np.isnan(delta):
                    raise RuntimeError("Invalid event series distribution.")
                if delta > maxDelta:
                    maxDelta = delta
                    removeIndex = i

            zippedList, eventSums = BucketConsolidator._combineAdjacentBins(zippedList, eventSums, removeIndex)

        # second pass, combine any adjacent buckets having lift within specified threshold(s)
        while len(zippedList) > 2:
            removeIndex = self._getNextConsolidationByLift(zippedList, eventSums)
            if removeIndex is None:
                break;
            zippedList, eventSums = BucketConsolidator._combineAdjacentBins(zippedList, eventSums, removeIndex)

        return [y[0] for y in zippedList] + [zippedList[-1][1]]
