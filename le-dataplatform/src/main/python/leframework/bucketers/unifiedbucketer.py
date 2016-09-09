import logging
import math

from numpy.random import choice
from scipy.stats import kurtosis

from bucketer import Bucketer
from leframework.codestyle import overrides


class UnifiedBucketer(Bucketer):


    def __init__(self):
        self.logger = logging.getLogger(name='unifiedbucketer')

    @overrides(Bucketer)
    def doConsolidation(self):
        return False

    @overrides(Bucketer)
    def bucketColumn(self, columnSeries, eventSeries, params):
        b = self.getBuckets(columnSeries, eventSeries)
        bc = self.combineBuckets(columnSeries, eventSeries, b)
        return self.fixFatTails(columnSeries, eventSeries, bc)

    def orderedIndicies(self, x, descending=False):
        return [y[0] for y in sorted(enumerate(x), key=lambda x: x[1], reverse=descending)]

    def getSig(self, sub, overall):
        subCnt, subRate = sub
        oaCnt, oaRate = overall
        r = (subCnt * subRate + oaCnt * oaRate) / (subCnt + oaCnt)
        if r in [0, 1]:
            return 0
        else:
            return (subRate - oaRate) / (math.sqrt(r * (1.0 - r) * (1.0 / subCnt + 1.0 / oaCnt)))

    def getTuple(self, eventCol):
        try:
            return (len(eventCol), sum(eventCol) * 1.0 / len(eventCol))
        except:
            return (len(eventCol), sum(float(x) for x in eventCol) / len(eventCol))
       
    def createIndexSequenceByValue(self, xList, eventList, rawSplits):
        # sp0 contains the index in xList where each bucket starts 
        xListLen = len(xList)
        sp0 = [0]
    
        # if rawbinnum >= # unique values, put each value in a bin
        if rawSplits >= len(set(xList)):
            sp0 = []
            previous_val = None
            for idx, x in enumerate(xList):
                if x != previous_val:
                    sp0.append(idx)
                    previous_val = x
            sp0.append(xListLen)
            return sp0
    
        posCurr = 0
        valCurr = xList[0]
        numPtsCurr = 0
        divider = 3

        stepSize = int(xListLen / (rawSplits * divider))
        numPtsInBin = stepSize * divider
    
        # if there is not enough data, no bucketing is needed
        if stepSize == 0:
            return [0, len(xList)]
       
        while posCurr < xListLen:
            # move the position by stepSize
            posCurr = min(posCurr + stepSize, xListLen)
            # if the value at current position is the same as the starting value of the current bucket
            if xList[posCurr - 1] == valCurr:
                numPtsCurr += stepSize
                continue
            elif xList[posCurr - 1] > valCurr:
                posTemp = len([x for x in xList[posCurr - stepSize:posCurr] if x == valCurr])
                # if the value at current position is larger than the starting value of the current bucket and there is enough points in the bucket, back up to make sure the same values fall into the same bucket
                if numPtsCurr + posTemp >= numPtsInBin:
                    sp0.append(posCurr - stepSize + posTemp)
                    numPtsCurr = stepSize - posTemp
                    valCurr = xList[posCurr - stepSize + posTemp]
                else:
                    numPtsCurr += stepSize
        sp0.append(xListLen)
    
        # this code adjust the index in sp0 such that the same values go into the bucket
        # needs a separate function for this
        sp0Length = len(sp0)
        bucket = 1
        while bucket < sp0Length - 1:
            if xList[sp0[bucket] - 1] == xList[sp0[bucket]]:
                xListInBucket = xList[sp0[bucket - 1]:sp0[bucket]]
                sp0[bucket] = sp0[bucket] - len([x for x in xListInBucket if x == xList[sp0[bucket]]])
            bucket = bucket + 1
        
        if sp0[-1] < len(xList):
            sp0.append(len(xList))

        return sp0
    
    def createIndexSequence(self, xList, eventList, rawSplits, sigThreshold, minNumBucket, maxNumBucket):
        sp = self.createIndexSequenceByValue(xList, eventList, rawSplits)
        while len(sp) > minNumBucket + 1:
            rateTupleList = [self.getTuple(eventList[sp[b]:sp[b + 1]]) for b in range(len(sp) - 1)]
            sigList = [math.fabs(self.getSig(rateTupleList[b], rateTupleList[b + 1])) for b in range(len(rateTupleList) - 1)]
            sigListOrdIdx = self.orderedIndicies(sigList)

            if len(sp) <= maxNumBucket + 1 and min(sigList) > 0:
                break
        
            bucketToDel = []
        
            for idx in sigListOrdIdx:
                if sigList[idx] >= sigThreshold or len(sp) - 1 - len(bucketToDel) <= minNumBucket:
                    break
                elif idx not in sum([[x - 1, x, x + 1] for x in bucketToDel], []):
                    bucketToDel.append(idx)
           
            bucketToDel = set([x + 1 for x in bucketToDel])
            sp = [sp[i] for i in range(len(sp)) if i not in bucketToDel] 
        
            if len(bucketToDel) == 0:
                print("all buckets have significance larger than current threshold %s" % sigThreshold)
                break
      
        return tuple(sp)
    
    def getBuckets(self, columnData, eventSeries, rawBinNumber=200, minPtsToBucket=50, sigThreshold=8, minNumBucket=1, maxNumBucket=200, minSpecialValRatio=0.02, minRegBucketSize=20):
        # handle special cases
        specialValues = []
        if any(math.isnan(x) for x in columnData):
            minNumBucket = minNumBucket - 1
            maxNumBucket = maxNumBucket - 1
        if (sum([x == 0 for x in columnData]) >= minSpecialValRatio * len(columnData)):
            minNumBucket = minNumBucket - 1
            maxNumBucket = maxNumBucket - 1
            specialValues.append(0)
        if sum([x == 1 for x in columnData]) >= minSpecialValRatio * len(columnData):
            minNumBucket = minNumBucket - 1
            maxNumBucket = maxNumBucket - 1
            specialValues.append(1)
        if minNumBucket < 0:
            minNumBucket = 0
        if maxNumBucket < 0:
            maxNumBucket = 0

        xListToBucket = [x for x in columnData if not math.isnan(x) and  x not in specialValues]
    
        # all values are the same, or list empty
        if len(set(xListToBucket)) <= 1:
            return ['NA' if math.isnan(x) else "%s" % x for x in columnData]
        # not enough data to bucket
        elif len(xListToBucket) <= minPtsToBucket: 
            xListNAMin = min(xListToBucket)
            xListNAMax = max(xListToBucket)
            return ['NA' if math.isnan(x) else str(x) if x in specialValues else "[%s, %s]" % (xListNAMin, xListNAMax) for x in columnData]
        else: 
            if len(xListToBucket) / rawBinNumber < minRegBucketSize:
                rawBinNumber = int((len(xListToBucket)) / minRegBucketSize)
        
            idxOrdered = self.orderedIndicies(xListToBucket)
        
            xListToBucket_ord = [xListToBucket[i] for i in idxOrdered]
        
            eventListToBucket = [x[0] for x in zip(eventSeries, columnData) if not math.isnan(x[1]) and x[1] not in specialValues]
        
            eventListToBucket_ord = [eventListToBucket[i] for i in idxOrdered]
            binIndex = self.createIndexSequence(xListToBucket_ord, eventListToBucket_ord, rawBinNumber, sigThreshold, minNumBucket, maxNumBucket)
        
            return ['NA' if math.isnan(x) else str(x) if x in specialValues else "[%s, %s]" % self.getBound(x, binIndex, xListToBucket_ord) for x in columnData]
        

    def combineBuckets(self, columnData, eventVector, buckets, sigThreshold=8, minNumBucket=1, maxNumBucket=10):
        # find non-special buckets
        regularBuckets = sorted([(float(x[1:x.find(",")]), float(x[x.find(",") + 2:-1])) for x in set(buckets) if x[0] == '['])
        # return if already good, or no buckets to combine
        if len(set(buckets)) <= maxNumBucket or len(regularBuckets) <= 1:
            return buckets

        bucketSet = set(buckets)
        currNumBuckets = len(bucketSet)
        eventsInBuckets = {bucket:[] for bucket in bucketSet}
        for b, e in zip(buckets, eventVector):
            # find corresponding event vectors for each regular bucket
            eventsInBuckets[b].append(e)
        rateTupleList = []

        for bucket in regularBuckets:
            # find corresponding event vectors for each regular bucket
            eventsInBucket = eventsInBuckets["[%s, %s]" % (bucket[0], bucket[1])]
            rateTupleList.append((len(eventsInBucket), float(sum(eventsInBucket)) / len(eventsInBucket)))

        sigList = [math.fabs(self.getSig(rateTupleList[b], rateTupleList[b + 1])) for b in range(len(rateTupleList) - 1)]

        while currNumBuckets > minNumBucket:
            minSig = min(sigList)
            if minSig >= sigThreshold:
                print "all buckets have significance larger than current threshold %s" % sigThreshold
                break
            else:
                # find an index to delete, to avoid bias, delete a random one of multiple mins exist
                minIndices = [i for i, x in enumerate(sigList) if x == minSig]
                indexToDel = choice(minIndices, 1)[0]
                # update bucket updating rules
                mergedBucket = (regularBuckets[indexToDel][0], regularBuckets[indexToDel + 1][1])
                regularBuckets[indexToDel] = mergedBucket
                regularBuckets.pop(indexToDel + 1)

                # update stats and sig values of adjacent buckets
                newBucketSize = rateTupleList[indexToDel][0] + rateTupleList[indexToDel + 1][0]
                newBucketEvents = rateTupleList[indexToDel][0] * rateTupleList[indexToDel][1] + rateTupleList[indexToDel + 1][0] * rateTupleList[indexToDel + 1][1]
                rateTupleList[indexToDel] = (newBucketSize, float(newBucketEvents) / newBucketSize)
                rateTupleList.pop(indexToDel + 1)
                # update prior sig if any
                if indexToDel > 0:
                    sigList[indexToDel - 1] = math.fabs(self.getSig(rateTupleList[indexToDel - 1], rateTupleList[indexToDel]))
                # update next sig if any
                if indexToDel < len(sigList) - 1:
                    sigList[indexToDel + 1] = math.fabs(self.getSig(rateTupleList[indexToDel], rateTupleList[indexToDel + 1]))
                # remove deleted boundary
                sigList.pop(indexToDel)

                currNumBuckets = currNumBuckets - 1
                if currNumBuckets <= maxNumBucket:
                    break

        # update buckets
        bucketDict = self.getBucketDictionary(bucketSet, regularBuckets, specialValues=['NA', '1.0', '0.0'])
        buckets = [bucketDict[bucket] for bucket in buckets]
    
        return buckets

    def getBucketDictionary(self, bucketSet, intervals, specialValues):
        bucketDict = {}
        for bucket in bucketSet:
            if bucket in specialValues:
                bucketDict[bucket] = bucket
            else:
                for interval in intervals:
                    if interval[0] <= float(bucket[1:bucket.find(",")]) and interval[1] >= float(bucket[bucket.find(",") + 2:-1]):
                        bucketDict[bucket] = "[%s, %s]" % (interval[0], interval[1])
        return bucketDict

    def getBound(self, y, boundaries, xList_ord):
        for i in range(len(boundaries) - 1):
            if xList_ord[boundaries[i]] <= y and xList_ord[boundaries[i + 1] - 1] >= y: 
                return (float(xList_ord[boundaries[i]]), float(xList_ord[boundaries[i + 1] - 1]))

    def fixFatTails(self, columnData, eventVector, buckets, kurtThreshold=3, minPctToSplit=2, splitPct=1, numAdditionalBuckets=2):
        # no need to fix if there's no fat tail
        if kurtosis(columnData) < kurtThreshold:
            return buckets

        # find non-special buckets
        regularBuckets = sorted([(float(x[1:x.find(",")]), float(x[x.find(",") + 2:-1])) for x in set(buckets) if x[0] == '['])
        if len(regularBuckets) <= 1:
            return buckets


        # find the last bucket
        lastBucket = regularBuckets[-1]
        idxLastBucket = [b == "[%s, %s]" % (lastBucket[0], lastBucket[1]) for b in buckets]
    
        # don't split if last segment is small
        if sum(idxLastBucket) < minPctToSplit * 0.01 * len(columnData):
            return buckets
    
        splits = min(int(sum(idxLastBucket) / (splitPct * 0.01 * len(columnData))), numAdditionalBuckets + 1)
        xList = [x for (x, i) in zip(columnData, idxLastBucket) if i]
        eventList = [e for (e, i) in zip(eventVector, idxLastBucket) if i]    
        idxOrdered = self.orderedIndicies(xList)
        xList_ord = [xList[i] for i in idxOrdered]
        eventList_ord = [eventList[i] for i in idxOrdered]
        boundaries = self.createIndexSequenceByValue(xList_ord, eventList_ord, splits)
    
        newTail = ["[%s, %s]" % self.getBound(x, boundaries, xList_ord) for x in xList]
        for i, inTail in enumerate(idxLastBucket):
            if inTail:
                buckets[i] = newTail.pop(0)

        return buckets
