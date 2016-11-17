import pandas as pd
import math
from numpy import floor, log10, ceil
from numpy.random import choice

from itertools import compress, product
from sklearn.metrics.cluster.supervised import entropy
from scipy.stats import kurtosis, skew

# generates the ordering of column data so we can sort event vector the same way
def orderedIndicies(x, descending = False):
    return [y[0] for y in sorted(enumerate(x), key = lambda x: x[1], reverse = descending)]

# significance of difference in conversation rates between two neighboring buckets
def getSig(sub, overall):
    subCnt, subPos = sub
    oaCnt, oaPos = overall
    subRate = float(subPos) / subCnt
    oaRate = float(oaPos) / oaCnt
    r = (subCnt*subRate + oaCnt*oaRate)/(subCnt+oaCnt)
    if r in [0, 1]:
        return 0
    else:
        return abs(subRate-oaRate)/(math.sqrt(r*(1.0-r)*(1.0/subCnt + 1.0/oaCnt)))

# generates sample and event count for in a bucket
def getCountsTuple(eventCol):
    try:
        return (len(eventCol), sum(eventCol))
    except:
        return (len(eventCol), sum(float(x) for x in eventCol))

# uncertainty coefficient from mi and entropy
def uncertaintyCoeff(mi, entropy):
    if mi == None or entropy == 0:
        return None
    return mi / entropy

def roundTo(x, sigDigits=2):
    if x==0:
        return 0
    else:
        return round(x, sigDigits - 1 - int(floor(log10(abs(x)))))

def getBound(y, boundaries):
    for idx in range(len(boundaries) - 1):
        if boundaries[idx+1] > y:
            return boundaries[idx]

def truncateToFixedBoundaries(x):
    multipliers = [1, 2, 5]
    maxInput = max(max(x), 10)
    maxBase = int(floor(log10(abs(maxInput))))
    bases = [math.pow(10,b) for b in range(maxBase + 1)]
    combinations = list(product(multipliers, bases))
    boundaries = sorted(c[0]*c[1] for c in combinations)
    boundaries.append(math.pow(10, maxBase + 1))
    xTruncated = [getBound(y, boundaries) for y in x]
    maxVal = min([b for b in boundaries if b > getBound(maxInput, boundaries)])
    return maxVal, xTruncated

# find where the next value is different
# direction = 1 searches to the right
# direction = -1 searches to the left
def findWhereValueChanges(xList, xListLen, idx, direction):
    if idx < 0 or idx >= xListLen:
        print "invalid searching starting point"
        return None
    if direction not in [-1, 1]:
        print "invalid searching direction"
        return idx
    while idx + direction > 0 and idx + direction <xListLen:
        if xList[idx + direction] == xList[idx]:
            idx = idx + direction
        else:
            break
    # return the index of first occurence of a different x value
    # if already at the end, return the end point
    return min( max(idx + direction, 0), xListLen - 1)

def createBinsByStepSize(xList, xListLen, stepSize, numPtsInBin):
    binIdx = [0]
    posCurr = 0
    while posCurr < xListLen:
        # move the position by stepSize
        posCurr = posCurr + stepSize
        # end iteration when reaching the end
        if posCurr >= xListLen:
            binIdx.append(xListLen)
            break
        # if the value at current position is the same as the starting value 
        # of the current bucket, or there are still not sufficient points to make
        # a new bucket, keep expanding current bucket
        elif xList[posCurr] == xList[binIdx[-1]] or posCurr - binIdx[-1] < numPtsInBin:
            continue
        # when a new bucket is possible, check the new boundary point
        # if values at each side of the boundary point are different
        # create a new bucket
        elif xList[posCurr] > xList[posCurr-1]:
            binIdx.append(posCurr)
        # potential boundary separates the same value into two bins
        # need to move it
        else:
            # find position before first appearance of boundary value
            idxL = findWhereValueChanges(xList, xListLen, posCurr, -1)            
            # if there are enough points without boundary values to make a bucket, 
            # back up to the left of first occurence of boundary values
            # make a new bucket and start from there
            if idxL + 1 - binIdx[-1] >= numPtsInBin:                
                posCurr = idxL + 1
                binIdx.append(posCurr)
            # otherwise, merge all boundary value occrences into left bucket
            else:
                posCurr = findWhereValueChanges(xList, xListLen, posCurr, 1)
                binIdx.append(posCurr)
    return binIdx

# split into roughly equally sized buckets
def createRawBins(xList, eventList, rawSplits):    
    xListLen = len(xList)
    divider = 3
    stepSize = int(xListLen/(rawSplits*divider))
    numPtsInBin = stepSize * divider
    #if there is not enough data, no bucketing is needed
    if stepSize == 0:
        binIdx = [0, len(xList)]
    # all other cases - make bins by step size
    else:        
        binIdx = createBinsByStepSize(xList, xListLen, stepSize, numPtsInBin)
    return binIdx

def identifySpecialValues(columnData, eventVector, minSpecialValRatio, forUI=False):
    specialValues = []
    if any(math.isnan(x) for x in columnData):
        specialValues.append(None)
    if (not forUI) and (0 in columnData):
        specialValues.append(0)
    oneCnt = columnData.count(1)
    if (not forUI) and oneCnt >= minSpecialValRatio * len(columnData):
        specialValues.append(1)
    return specialValues

def addBucketsToOutput(bandsDict, mins, maxes, splCnts, evtCnts):
    bandsDict["min"].extend(mins)
    bandsDict["max"].extend(maxes)
    bandsDict["sampleCount"].extend(splCnts)
    bandsDict["eventCount"].extend(evtCnts)
    return bandsDict
    
def processSpecialValues(columnData, eventVector, specialValues):
    #add special cases to stats
    bandsDict = {"min":[], "max":[], "sampleCount":[], "eventCount":[]}
    for sv in specialValues:
        if sv is None:
            svEvts = [x[0] for x in zip(eventVector, columnData) if math.isnan(x[1])]
        else:
            svEvts = [x[0] for x in zip(eventVector, columnData) if x[1]==sv]
        bandsDict = addBucketsToOutput(bandsDict, [sv], [sv], [len(svEvts)], [sum(svEvts)])
    #get remaining values to bucket
    xIdxToBucket = [not math.isnan(x) and x not in specialValues for x in columnData]
    xListToBucket = list(compress(columnData, xIdxToBucket))
    eventListToBucket = list(compress(eventVector, xIdxToBucket))
    
    return xListToBucket, eventListToBucket, bandsDict
    
def createRegularBuckets(xListToBucket, eventListToBucket, totalCnt, minPtsToBucket, sigThreshold,
                         rawBinNumber, minRegBucketSize, minNumBuckets, maxNumBuckets, 
                         forUI, kurtThreshold, tailSize, numTailBuckets, sizeThreshold, 
                         liftThreshold, evevtDiffThreshold):
    #all values are the same, or list empty
    if len(set(xListToBucket)) == 0:
        bktStats, bucketMins, bucketMaxes = [], [], []
    # single uniqe value, or not enough data to bucket
    elif len(set(xListToBucket)) == 1 or len(xListToBucket) <= minPtsToBucket:
        bktStats = [(len(eventListToBucket), sum(eventListToBucket))]
        bucketMins, bucketMaxes = [min(xListToBucket)], [max(xListToBucket)]
    # bucket values
    else: 
        if len(xListToBucket)/rawBinNumber < minRegBucketSize:
            rawBinNumber = int((len(xListToBucket))/minRegBucketSize)
        # sort x List and event list
        idxOrdered = orderedIndicies(xListToBucket)
        xListToBucket_ord = [xListToBucket[i] for i in idxOrdered]
        eventListToBucket_ord = [eventListToBucket[i] for i in idxOrdered]

        # generate roughly equally sized bins
        binIndex = createRawBins(xListToBucket_ord, eventListToBucket_ord, rawBinNumber)
        # calculate sample counts and event counts in each bin
        bktStats = [getCountsTuple(eventListToBucket_ord[binIndex[b]:binIndex[b+1]]) for b in range(len(binIndex)-1)]
        #fix fat tails for UI
        if forUI:
            #consider kurtosis, tails, lift, min bcket size etc
            highKurtosis = kurtosis(xListToBucket, fisher=False) >= kurtThreshold
            positiveSkew = skew(xListToBucket) >= 0
            binIndex, bktStats = combineUIBuckets(binIndex, bktStats, totalCnt, sigThreshold, 
                                        minNumBuckets, maxNumBuckets, highKurtosis, positiveSkew,
                                        tailSize, numTailBuckets, sizeThreshold, 
                                        liftThreshold, evevtDiffThreshold)
            
        else:
            # combine bins directly
            binIndex, bktStats = combineBuckets(binIndex, bktStats, sigThreshold, 
                                                minNumBuckets, maxNumBuckets, forUI=False)
        # generate buckets
        bucketMins = [xListToBucket_ord[binIndex[i]] for i in range(len(binIndex)-1)]
        bucketMaxes = [xListToBucket_ord[binIndex[i+1]-1] for i in range(len(binIndex)-1)]
    return bktStats, bucketMins, bucketMaxes
    
def addVariablesToOutput(bandsDict, eventVector):
    #global stats used for later calculation
    entropyVal = entropy(eventVector)
    avgProbability = sum(eventVector) / float(len(eventVector))
    # calculate lift and mutual information for all buckets, regular and special
    miComponents = calculateMutualInfoBinary(bandsDict["sampleCount"], bandsDict["eventCount"])
    bandsDict["uncertaintyCoefficient"] = [uncertaintyCoeff(mi, entropyVal) for mi in miComponents]
    convRates = [float(x[0]) / x[1] for x in zip(bandsDict["eventCount"], bandsDict["sampleCount"])]
    bandsDict["lift"] = [x / avgProbability for x in convRates]    
    # add global values that don't change by band
    bandsDict["mi"] = sum(miComponents)
    bandsDict["entropyValue"] = entropyVal

    return bandsDict
   
def deleteIndex(binIndex, bktStats, indexToDel):
    # remove deleted bin index
    binIndex.pop(indexToDel + 1)
    #update stats of adjacent buckets
    newBucketSize = bktStats[indexToDel][0] + bktStats[indexToDel + 1][0]
    newBucketEvents = bktStats[indexToDel][1] + bktStats[indexToDel + 1][1]
    bktStats[indexToDel] = (newBucketSize, newBucketEvents)
    #remove deleted bucket stats
    bktStats.pop(indexToDel + 1)
    return binIndex, bktStats

def combineBuckets(binIndex, bktStats, sigThreshold, minNumBuckets, maxNumBuckets, forUI):
    #check current number of buckets
    currNumBuckets = len(bktStats)
    if currNumBuckets <= max(maxNumBuckets, 1):
        return binIndex, bktStats
        
    sigList = [getSig(bktStats[b],bktStats[b+1]) for b in range(len(bktStats)-1)]
    
    while currNumBuckets > minNumBuckets:
        minSig = min(sigList)
        if not forUI and minSig >= sigThreshold:
            break
        else:
            # find an index to delete, to avoid bias, delete a random one of multiple mins exist
            minIndices = [i for i, x in enumerate(sigList) if x == minSig]
            indexToDel = choice(minIndices, 1)[0]
            # remove deleted bin index and bktStats
            binIndex, bktStats = deleteIndex(binIndex, bktStats, indexToDel)
            # update prior sig if any
            if indexToDel > 0:
                sigList[indexToDel - 1] = getSig(bktStats[indexToDel - 1],bktStats[indexToDel])
            # update next sig if any
            if indexToDel < len(sigList) - 1:
                sigList[indexToDel + 1] = getSig(bktStats[indexToDel],bktStats[indexToDel + 1])
            # remove deleted sig value
            sigList.pop(indexToDel)

            currNumBuckets = currNumBuckets - 1
            if currNumBuckets <= maxNumBuckets:
                #print rateTupleList
                break
    return binIndex, bktStats

def combineUIBuckets(binIndex, bktStats, totalCnt, sigThreshold, minNumBuckets, maxNumBuckets,
                     highKurtosis, positiveSkew, tailSize, numTailBuckets, sizeThreshold, 
                     liftThreshold, evevtDiffThreshold):
    # check current number of buckets
    currNumBuckets = len(bktStats)
    if currNumBuckets <= max(minNumBuckets, 1):
        return binIndex, bktStats

    # combines by significance, bucket the tail separately if high kurtosis
    if highKurtosis:
        tailCnts = 0
        binIndexTail = []
        bktStatsTail = []
        if positiveSkew:
            while tailCnts < tailSize * totalCnt:
                binIndexTail = [binIndex.pop()] + binIndexTail
                bktInTail = bktStats.pop()
                tailCnts = tailCnts + bktInTail[0]
                bktStatsTail = [bktInTail] + bktStatsTail
            binIndexTail = [binIndex[-1]] + binIndexTail
        else: #tail is just head in this case
            while tailCnts < tailSize * totalCnt:
                binIndexTail = binIndexTail + [binIndex.pop(0)]
                bktInTail = bktStats.pop(0)
                tailCnts = tailCnts + bktInTail[0]
                bktStatsTail = bktStatsTail + [bktInTail]
            binIndexTail = binIndexTail + [binIndex[0]]

        binIndexTail, bktStatsTail = combineBuckets(binIndexTail, bktStatsTail, sigThreshold, 
                                                    minNumBuckets=1, 
                                                    maxNumBuckets=numTailBuckets, forUI=True)
        minNumBuckets = minNumBuckets - len(bktStatsTail)
        maxNumBuckets = maxNumBuckets - len(bktStatsTail)
        maxNumBuckets = max(1, maxNumBuckets)
        minNumBuckets = max(1, minNumBuckets)
        binIndex, bktStats = combineBuckets(binIndex, bktStats, sigThreshold, minNumBuckets, 
                                            maxNumBuckets, forUI=True)

        binIndex, bktStats = removeSmallBucketsByLift(binIndex, bktStats, totalCnt, minNumBuckets, 
                                                      sizeThreshold, liftThreshold)
        removeInsignificantBucketsByEventCount(binIndex, bktStats, minNumBuckets,evevtDiffThreshold)
        if positiveSkew:
            binIndex = binIndex + binIndexTail[1:]
            bktStats = bktStats + bktStatsTail
        else:
            binIndex = binIndexTail[:-1] + binIndex
            bktStats = bktStatsTail + bktStats

    else:
        binIndex, bktStats = combineBuckets(binIndex, bktStats, sigThreshold, minNumBuckets, 
                                            maxNumBuckets, forUI=True)
        binIndex, bktStats = removeSmallBucketsByLift(binIndex, bktStats, totalCnt, minNumBuckets, 
                                                      sizeThreshold, liftThreshold)
        binIndex, bktStats = removeInsignificantBucketsByEventCount(binIndex, bktStats, 
                                                                    minNumBuckets,evevtDiffThreshold)
    
    return binIndex, bktStats

def getLiftDiff(bkta, bktb, threshold):
    cntA, posA = bkta
    cntB, posB = bktb
    rateA = float(posA) / cntA
    rateB = float(posB) / cntB
    rateA, rateB = sorted([rateA, rateB])
    # both are 0
    if rateB == 0:
        return 0
    # one is zero and the other is not
    elif rateA == 0:
        return threshold + 1
    else:
        return rateB/rateA

# remove small buckets
def removeSmallBucketsByLift(binIndex, bktStats, totalCnt, minNumBuckets, sizeThreshold, liftThreshold):
    tryDelete = True
    while tryDelete:
        sizes = [1.0 * x[0] / totalCnt for x in bktStats]
        liftDiffs = [getLiftDiff(bktStats[b],bktStats[b+1], liftThreshold) for b in range(len(bktStats)-1)]
        indicesToDel = []
        for idx, ld in enumerate(liftDiffs):
            if ld < liftThreshold and  (sizes[idx] < sizeThreshold or sizes[idx+1] < sizeThreshold):
                indicesToDel.append(tuple([ld, idx]))
        if indicesToDel and len(bktStats) > minNumBuckets:
            indicesToDel.sort()
            indexToDel = indicesToDel[0][1]
            binIndex, bktStats = deleteIndex(binIndex, bktStats, indexToDel)
        else:
            tryDelete = False
    return binIndex, bktStats           

def getEventDiff(bkta, bktb):
    cntA, posA = bkta
    cntB, posB = bktb
    avgRate = (posA + posB) / (cntA + cntB)
    expPosA = avgRate * cntA
    expPosB = avgRate * cntB
    evtDiff = abs((posA - expPosA) - (posB - expPosB))
    return evtDiff

def removeInsignificantBucketsByEventCount(binIndex, bktStats, minNumBuckets,evevtDiffThreshold):
    tryDelete = True
    while tryDelete:
        eventDiffs = [getEventDiff(bktStats[b],bktStats[b+1]) for b in range(len(bktStats)-1)]
        indicesToDel = []
        for idx, evtdiff in enumerate(eventDiffs):
            if evtdiff < evevtDiffThreshold:
                indicesToDel.append(tuple([evtdiff, idx]))
        if indicesToDel and len(bktStats) > minNumBuckets:
            indicesToDel.sort()
            indexToDel = indicesToDel[0][1]
            binIndex, bktStats = deleteIndex(binIndex, bktStats, indexToDel)
        else:
            tryDelete = False
    return binIndex, bktStats

# make boundaries continuous
def makeBoundariesContinuous(mins, maxes):
    for idx in range(len(mins) - 1):
        if maxes[idx] is not None:
            maxes[idx] = mins[idx+1]
    return mins, maxes

def calculateMutualInfoBinary(splCnts, posCnts):
    totalLength,totalPositives=sum(splCnts),sum(posCnts)
    rate=[float(totalLength-totalPositives)/totalLength,float(totalPositives)/totalLength]
    def relative_dep(splCnt, posCnt):
         joint_prob=[float(splCnt - posCnt)/totalLength,float(posCnt)/totalLength]
         p_x = float(splCnt)/totalLength
         returnVal=0.0
         for yi in range(2):
            returnVal += joint_prob[yi] * math.log(joint_prob[yi] / (p_x * rate[yi])) if joint_prob[yi] != 0.0 else 0.0
         return returnVal
    mi_components=[relative_dep(splCnt, posCnt) for splCnt, posCnt in zip(splCnts, posCnts)]
    return mi_components
    
def getBucketsAndStats(columnData, eventVector, rawBinNumber = 200, minPtsToBucket = 50,
                       minSpecialValRatio = 0.02, minRegBucketSize = 20, minRegBucketRatio = 0.01,
                       sigThreshold = 8, minNumBuckets = 3, maxNumBuckets = 7, forUI = False,
                       kurtThreshold=3, numTailBuckets=2, tailSize=0.05, sizeThreshold=0.05,
                       liftThreshold=1.8, fixBoundaries=False, evevtDiffThreshold=10):
    totalCnt = len(columnData)
    # process special values
    specialValues = identifySpecialValues(columnData, eventVector, minSpecialValRatio, forUI)    
    xListToBucket, eventListToBucket, bandsDict = processSpecialValues(columnData, eventVector, specialValues)
    maxNumBuckets = maxNumBuckets - len(bandsDict["min"])
    minNumBuckets = minNumBuckets - len(bandsDict["min"])
    maxNumBuckets = max(1, maxNumBuckets)
    minNumBuckets = max(1, minNumBuckets)
    # use round edges
    if forUI and fixBoundaries:
        maxValFixBound, xListToBucket = truncateToFixedBoundaries(xListToBucket)
    elif forUI:
        xListToBucket = [roundTo(x, 2) for x in xListToBucket]
    # add regular buckets
    minRegBucketSize = max(minRegBucketSize, ceil(minRegBucketRatio * len(columnData)))
    bktStats, bucketMins, bucketMaxes = createRegularBuckets(xListToBucket, 
                         eventListToBucket, totalCnt, minPtsToBucket, sigThreshold,
                         rawBinNumber, minRegBucketSize, minNumBuckets, maxNumBuckets, 
                         forUI, kurtThreshold, tailSize, numTailBuckets, sizeThreshold, 
                         liftThreshold, evevtDiffThreshold)
    bandsDict = addBucketsToOutput(bandsDict
                                   ,bucketMins
                                   ,bucketMaxes
                                   ,[x[0] for x in bktStats]
                                   ,[x[1] for x in bktStats])
    if forUI:
        # make boundaries continuous
        if fixBoundaries:
            bandsDict["max"][-1] = maxValFixBound
        bandsDict["min"], bandsDict["max"] = makeBoundariesContinuous(bandsDict["min"], bandsDict["max"])
    # add additional variables such as uncertainty coefficient etc.
    bandsDict = addVariablesToOutput(bandsDict, eventVector)
    
    return bandsDict
