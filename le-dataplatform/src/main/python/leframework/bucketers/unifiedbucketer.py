import logging
import math

from numpy.random import choice
from itertools import compress

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
        specialValues, regularBuckets, rateTupleList, sigList = self.getBuckets(columnSeries.tolist(), eventSeries)
        specialValues, regularBuckets = self.combineBuckets(specialValues, regularBuckets, rateTupleList, sigList, sigThreshold = 8, minNumBucket = 1, maxNumBuckets = 10)
        return self.convertBucketFormat(specialValues, regularBuckets)

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
    
    def getBuckets(self, columnData, eventVector, rawBinNumber = 200, minPtsToBucket = 50, minSpecialValRatio = 0.02, minRegBucketSize = 20):
        #handle special cases
        specialValues = []
        if any(math.isnan(x) for x in columnData):
            specialValues.append('NA')
        if 0 in columnData:
            specialValues.append(0)
        if columnData.count(1) >= minSpecialValRatio * len(columnData):
            specialValues.append(1)
    
        xIdxToBucket = [not math.isnan(x) and x not in specialValues for x in columnData]
        xListToBucket = list(compress(columnData, xIdxToBucket))
    
        xListToBucket_ord = None        
        rateTupleList = None
        sigList = None

        #all values are the same, or list empty
        if len(set(xListToBucket)) == 0:
            regularBuckets = []
        # single uniqe value, or not enough data to bucket
        elif len(set(xListToBucket)) == 1 or len(xListToBucket) <= minPtsToBucket:
            regularBuckets = [(float(min(xListToBucket)), float(max(xListToBucket)))]
        else: 
            if len(xListToBucket)/rawBinNumber < minRegBucketSize:
                rawBinNumber = int((len(xListToBucket))/minRegBucketSize)
            
            idxOrdered = self.orderedIndicies(xListToBucket)        
            xListToBucket_ord = [xListToBucket[i] for i in idxOrdered]
    
            eventListToBucket = list(compress(eventVector, xIdxToBucket))
            eventListToBucket_ord = [eventListToBucket[i] for i in idxOrdered]
    
            binIndex = self.createIndexSequenceByValue(xListToBucket_ord, eventListToBucket_ord, rawBinNumber)
            
            rateTupleList = [self.getTuple(eventListToBucket_ord[binIndex[b]:binIndex[b+1]]) for b in range(len(binIndex)-1)]
            sigList = [math.fabs(self.getSig(rateTupleList[b],rateTupleList[b+1])) for b in range(len(rateTupleList)-1)]

            regularBuckets= [(float(xListToBucket_ord[binIndex[i]]), float(xListToBucket_ord[binIndex[i+1]-1])) for i in range(len(binIndex)-1)]
            
        return specialValues, regularBuckets, rateTupleList, sigList
            
    #@profile
    def combineBuckets(self, specialValues, regularBuckets, rateTupleList, sigList, sigThreshold = 8, minNumBucket = 1, maxNumBuckets = 10):
        #find non-special buckets
        currNumBuckets = len(specialValues) + len(regularBuckets)
                
        #if len(binIndex) <= maxNumBucket or len(regularBuckets) <= 1:
        if currNumBuckets <= maxNumBuckets or len(regularBuckets) < 2:
            return specialValues, regularBuckets
    
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
                #update stats and sig values of adjacent buckets
                newBucketSize = rateTupleList[indexToDel][0] + rateTupleList[indexToDel + 1][0]
                newBucketEvents = rateTupleList[indexToDel][0] * rateTupleList[indexToDel][1] + rateTupleList[indexToDel + 1][0] * rateTupleList[indexToDel + 1][1]
                rateTupleList[indexToDel] = (newBucketSize, float(newBucketEvents) / newBucketSize)
                rateTupleList.pop(indexToDel + 1)
                # update prior sig if any
                if indexToDel > 0:
                    sigList[indexToDel - 1] = math.fabs(self.getSig(rateTupleList[indexToDel - 1],rateTupleList[indexToDel]))
                # update next sig if any
                if indexToDel < len(sigList) - 1:
                    sigList[indexToDel + 1] = math.fabs(self.getSig(rateTupleList[indexToDel],rateTupleList[indexToDel + 1]))
                # remove deleted boundary
                sigList.pop(indexToDel)
    
                currNumBuckets = currNumBuckets - 1
                if currNumBuckets <= maxNumBuckets:
                    break
                
        return specialValues, regularBuckets
    
    def convertBucketFormat(self, specialValues, regularBuckets):
        epsilon = 0.000001
        if 'NA' in specialValues:
            specialValues.remove('NA')
    
        if len(regularBuckets) < 1:
            return sorted(specialValues) + [max(specialValues) + epsilon]
    
        bounds = [b for seg in regularBuckets for b in seg]
    
        for sv in specialValues:
            if sv < bounds[0]:
                bounds = [sv, sv] + bounds
            elif sv > bounds[-1]:
                bounds = bounds + [sv, sv]
            else:
                for idx, val in enumerate(bounds):
                    if val > sv:
                        if idx % 2 == 1:
                            bounds[idx:idx] = [float(sv)-epsilon, float(sv), float(sv), float(sv)+epsilon]
                        else:
                            bounds[idx:idx] = [float(sv)-epsilon, float(sv)+epsilon]
                        break
        boundaries = [bounds[0]]
        for idx in range(1, len(bounds)-2, 2):
            boundaries.append( (bounds[idx] + bounds[idx+1]) / 2)
        boundaries.append(bounds[-1] + epsilon)
        
        return boundaries
