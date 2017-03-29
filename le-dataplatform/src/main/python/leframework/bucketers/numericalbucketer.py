from __future__ import division

from bisect import bisect_left
from itertools import compress, product
import math
from sklearn.metrics.cluster.supervised import entropy

from numpy import floor, log10, ceil, sign, fabs
from numpy.random import choice
from scipy.stats import kurtosis


# generates the ordering of column data so we can sort event vector the same way
def orderedIndicies(x, descending=False):
    """
    generates the ordering of column data so we can sort event vector the same way
    x: column data
    returns: list of positions for elements in x in a sorted list
    """
    return [y[0] for y in sorted(enumerate(x), key=lambda x: x[1], reverse=descending)]

# significance of difference in conversation rates between two neighboring buckets
def getSig(sub, overall):
    """
    significance of difference in conversation rates between two neighboring buckets
    sub: a tuple in the format of (sample_count, event_count)
    overall: same as sub and exchangeable, despite the name
    returns: significance of difference between the two bins
    """
    subCnt, subPos = sub
    oaCnt, oaPos = overall
    subRate = float(subPos) / subCnt
    oaRate = float(oaPos) / oaCnt
    r = (subCnt * subRate + oaCnt * oaRate) / (subCnt + oaCnt)
    if r in [0, 1]:
        return 0
    else:
        return abs(subRate - oaRate) / (math.sqrt(r * (1.0 - r) * (1.0 / subCnt + 1.0 / oaCnt)))

# generates sample and event count for in a bucket
def getCountsTuple(eventCol):
    """
    generates sample and event count for in a bucket
    eventCol: list of 0, 1 events
    returns: a tuple in the format of (sample_count, event_count)
    """
    try:
        return (len(eventCol), sum(eventCol))
    except:
        return (len(eventCol), sum(float(x) for x in eventCol))

# uncertainty coefficient from mi and entropy
def uncertaintyCoeff(mi, entropy):
    """
    calculates uncertainty coefficient from mi and entropy
    """
    if mi == None or entropy == 0:
        return None
    return mi / entropy

def getBound(y, boundaries):
    """
    returns the nearest lower boundary of a value, given a list of sorted 
    boundary values
    y: value
    boundaries: sorted boundaries, in ascending order
    returns: the largest boundary value that's still equal or less than y
    """
    for idx in range(len(boundaries) - 1):
        if boundaries[idx + 1] > y:
            return boundaries[idx]

def truncateToFixedBoundaries(x):
    """
    truncates a list of values to pre-defined boundary candidates
    x: list of values
    multipliers: a list of distinct values, larger than or equal to 1 but less than 10
    returns: maxVal - 
             since the all truncated values are less than actual max of x, this
             retains the fixed boundary that's just larger than all x values, for
             display purpose
             xTruncated-
             all values in x, mapped to the corresponding approximation in the
             format of multiplier * (10 ^ n), e.g. if multipliers are [1, 2, 5],
             52 will be mapped to 50 and 3578 will be mapped to 2000
    """    
    multipliers = [1, 2, 5]
    maxInput = max(max(x), 10)
    maxBase = int(floor(log10(abs(maxInput))))
    bases = [math.pow(10, b) for b in range(maxBase + 1)]
    combinations = list(product(multipliers, bases))
    boundaries = sorted(c[0] * c[1] for c in combinations)
    boundaries.append(math.pow(10, maxBase + 1))
    xTruncated = [getBound(y, boundaries) for y in x]
    maxVal = min([b for b in boundaries if b > getBound(maxInput, boundaries)])
    return maxVal, xTruncated

C_LOG5 = log10(0.5)

def roundTo5(x):
    """
    truncates a list of values to pre-defined boundary candidates
    x: list of values
    multipliers: a list of distinct values, larger than or equal to 1 but less than 10
    returns: maxVal - 
             since the all truncated values are less than actual max of x, this
             retains the fixed boundary that's just larger than all x values, for
             display purpose
             xTruncated-
             all values in x, mapped to the corresponding approximation in the
             format of multiplier * (10 ^ n), e.g. if multipliers are [1, 2, 5],
             52 will be mapped to 50 and 3578 will be mapped to 2000
    """
    if x == 0.0:
        return 0

    ax = fabs(x)
    l = log10(fabs(x))
    e = int(l)
    m0 = float(x) / 10.0**e
    if ax <= 10.0:
        n = 1 if l < 0.0 else 0
        m = round(m0, n)
        return round(m*10.0**e, 8)

    if m0 >= C_LOG5 and m0 < 0.0:
        precision = 2
    else: precision = 1

    n = precision if l < 0.0 else precision - 1
    m = round(float(2.0*x) / 10.0**e, n)
    return int(m*10.0**e)/2

def findWhereValueChanges(xList, xListLen, idx, direction):
    """
    find where the next value is different    
    xList: a sorted list of X
    xListLen: length of xList
    idx: idx to start searching
    direction = 1 searches to the right
    direction = -1 searches to the left
    returns: index of the next encountered element with different value from
             xList[idx]
    """
    if idx < 0 or idx >= xListLen:
        print "invalid searching starting point"
        return None
    if direction not in [-1, 1]:
        print "invalid searching direction"
        return idx
    while idx + direction > 0 and idx + direction < xListLen:
        if xList[idx + direction] == xList[idx]:
            idx = idx + direction
        else:
            break
    # return the index of first occurence of a different x value
    # if already at the end, return the end point
    return min(max(idx + direction, 0), xListLen - 1)

# split into roughly equally sized buckets
def createBinsBySampleCount(xList, rawSplits):    
    """
    Genertes a list of small bins as a first step in bucketing.
    Bins are roughly equally-sized, built from a sorted list.
    If a possible boundary straddles the same value, make sure the same value
    either belongs to the left side bin, or to the right side bin, or forms a 
    bin by itself.
    xList: a sorted list of X
    rawSplits: expected number of bins to make
    returns: a list of index positions at boundaries of these bins
    """
    xListLen = len(xList)
    divider = 3
    stepSize = int(xListLen / (rawSplits * divider))
    numPtsInBin = stepSize * divider
    # if there is not enough data, no bucketing is needed
    if stepSize == 0:
        binIdx = [0, len(xList)]
        return binIdx
    # all other cases - make bins by step size
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
        # a new bucket, or if the remaining buckets are not sufficient to make a 
        # new bucket, keep expanding current bucket
        elif xList[posCurr] == xList[binIdx[-1]] or posCurr - binIdx[-1] < numPtsInBin  \
        or xListLen - posCurr < numPtsInBin:
            continue
        # when a new bucket is possible, check the new boundary point
        # if values at each side of the boundary point are different
        # create a new bucket
        elif xList[posCurr] > xList[posCurr - 1]:
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

def logTransform(x):
    if x > 0:
        return log10(x)
    elif x == 0:
        return 0
    else:
        return -log10(-x)
    
# split into roughly equally sized buckets
def createBinsByValueRange(xList, rawSplits, useGeometric, minRegBucketSize):
    """
    Genertes a list of small bins as a first step in bucketing.
    Bins are roughly equally-sized, built from a sorted list.
    If a possible boundary straddles the same value, make sure the same value
    either belongs to the left side bin, or to the right side bin, or forms a 
    bin by itself.
    xList: a sorted list of X
    rawSplits: expected number of bins to make
    returns: a list of index positions at boundaries of these bins
    """
    xListLen = len(xList)
    # if there is not enough data, no bucketing is needed
    if xListLen <= minRegBucketSize:
        binIdx = [0, len(xList)]
        return binIdx
    # all other cases - make bins by range
    if useGeometric:
        xList = [logTransform(x) for x in xList]
    xMin = xList[0]
    xMax = xList[-1]
    boundaries = [xMin + (xMax - xMin) * x / rawSplits for x in range(1, rawSplits)]
                  
    binIdx = [0]
    for b in boundaries:
        bIdx = bisect_left(xList, b)
        if bIdx - binIdx[-1] >= minRegBucketSize:
            binIdx.append(bIdx)
    if xMax > binIdx[-1] - minRegBucketSize:
        binIdx.append(xListLen)
    else:
        binIdx[-1] = xListLen
    return binIdx
    
def identifySpecialValues(columnData, eventVector, minSpecialValRatio, forUI=False):
    """
Finds special values that require separate treatment.
columnData: input data, a numeric list
eventVector: a list of equivalent length compromising of only 1s and 0s
minSpecialValRatio: ratio threshold for a 'special value' to have its own bucket.
                Specifically, it's used for 1 in the current use case. NAs will
                always be in a separate bucket. 0s are always in a separate bucket
                when they are the minimum values. The special value status of 1
                in the UI use case is slightly more complicated -- it's only a 
                special value if threshold is met, and there are no possible buckets
                stranding 1.
forUI: if the use case is UI display
returns: list of special values
    """
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
    """
    Adds new buckets to existing output data structure.
    bandsDict: dictionary of 4 lists of equal length. given the same index, 
               each item in list correspond to a bucket.
    mins: new bucket mins to add
    maxes: new bucket maxes to add
    spltCnts: new bucket sample counts to add
    eventCnts: new bucket event counts to add
    returns: exdended bandsDict
    """
    bandsDict["min"].extend(mins)
    bandsDict["max"].extend(maxes)
    bandsDict["sampleCount"].extend(splCnts)
    bandsDict["eventCount"].extend(evtCnts)
    return bandsDict
    
def processSpecialValues(columnData, eventVector, specialValues):
    """
    Creates buckets for special values and remove them from next steps.
    columnData: input data, a numeric list
    eventVector: a list of equivalent length compromising of only 1s and 0s
    specialValues: list of values that have their own buckets  
    returns:
    xListToBucket- remaining values, sorted
    eventListToBucket- remaining events, sorted in the same order
    bandsDict - data structure storing current buckets.
           dictionary of 4 lists of equal length. given the same index, 
           each item in list correspond to a bucket.
    """
    # add special cases to stats
    bandsDict = {"min":[], "max":[], "sampleCount":[], "eventCount":[]}
    for sv in specialValues:
        if sv is None:
            svEvts = [x[0] for x in zip(eventVector, columnData) if math.isnan(x[1])]
        else:
            svEvts = [x[0] for x in zip(eventVector, columnData) if x[1] == sv]
        bandsDict = addBucketsToOutput(bandsDict, [sv], [sv], [len(svEvts)], [sum(svEvts)])
    # get remaining values to bucket
    xIdxToBucket = [not math.isnan(x) and x not in specialValues for x in columnData]
    xListToBucket = list(compress(columnData, xIdxToBucket))
    eventListToBucket = list(compress(eventVector, xIdxToBucket))
    
    return xListToBucket, eventListToBucket, bandsDict
    
def createRegularBucketsForMI(xListToBucket, eventListToBucket, totalCnt, minPtsToBucket, sigThreshold,
                         rawBinNumber, minRegBucketSize, minNumBuckets, maxNumBuckets):
    """
Create buckets out of non-special values.
xListToBucket- remaining values after special values are excluded, sorted
eventListToBucket- remaining events, sorted in the same order
totalCnt: count of all samples, including special values
rawBinNumber: the number of small bins to create. These small bins are grouped
             together by the algorithm to form the eventual buckets.
minPtsToBucket: if there are less than this number of points, everything is put
               into a single bucket.
minRegBucketSize: the smallest bucket size generated by the bucketing algorithm
                in normal circumstances.
sigThreshold: significance threshold to determine whether two adjacent bins are
               combined into the same bucket. For data science use case, if the
               threshold is met, combination will stop even if max number of buckets
               is exceeded. For UI use case, the max number of buckets specified 
               will always be adhered to.
minNumBuckets: the minimum number of buckets to generate in usual cases. This
               always takes precedence over sigThreshold. Exceptions where min
               buckets are ignored include: too few data, too few distinctive values,
               too few values with sufficient counts.
maxNumBuckets: the maximum number of buckets to generate in usual cases. In the
               UI use case this is always adhered to. In the data science use case,
               combination step stops once number of buckets is within maximum,
               but if sigThreshold is met, it could stop even when remaining 
               number of buckets is more than maximum.
returns:
bktStats - tuples in the format of (sample_count, event_count) for each bucket
bucketMins - min of values in each bucket
bucketMaxes - max of values in each bucket
    """
    minPtsToBucket = max(minPtsToBucket, minRegBucketSize * 2)
    # all values are the same, or list empty
    if len(set(xListToBucket)) == 0:
        bktStats, bucketMins, bucketMaxes = [], [], []
    # single uniqe value, or not enough data to bucket
    elif len(set(xListToBucket)) == 1 or len(xListToBucket) <= minPtsToBucket:
        bktStats = [(len(eventListToBucket), sum(eventListToBucket))]
        bucketMins, bucketMaxes = [min(xListToBucket)], [max(xListToBucket)]
    # bucket values
    else: 
        if len(xListToBucket) / rawBinNumber < minRegBucketSize:
            rawBinNumber = int((len(xListToBucket)) / minRegBucketSize)
        # sort x List and event list
        idxOrdered = orderedIndicies(xListToBucket)
        xListToBucket_ord = [xListToBucket[i] for i in idxOrdered]
        eventListToBucket_ord = [eventListToBucket[i] for i in idxOrdered]

        # generate roughly equally sized bins
        binIndex = createBinsBySampleCount(xListToBucket_ord, rawBinNumber)
        # calculate sample counts and event counts in each bin
        bktStats = [getCountsTuple(eventListToBucket_ord[binIndex[b]:binIndex[b + 1]]) for b in range(len(binIndex) - 1)]
        # combine bins
        binIndex, bktStats = combineBuckets(binIndex, bktStats, sigThreshold,
                                            minNumBuckets, maxNumBuckets, forUI=False)
        # generate buckets
        bucketMins = [xListToBucket_ord[binIndex[i]] for i in range(len(binIndex) - 1)]
        bucketMaxes = [xListToBucket_ord[binIndex[i + 1] - 1] for i in range(len(binIndex) - 1)]
    return bktStats, bucketMins, bucketMaxes

def createRegularBucketsForUI(xListToBucket, eventListToBucket, totalCnt, minPtsToBucket, sigThreshold,
                         rawBinNumber, minRegBucketSize, minNumBuckets, maxNumBuckets,
                         kurtThreshold, tailSize, numTailBuckets, sizeThreshold,
                         liftThreshold, eventDiffThreshold):
    """
Create buckets out of non-special values for UI.
xListToBucket- remaining values after special values are excluded, sorted
eventListToBucket- remaining events, sorted in the same order
totalCnt: count of all samples, including special values
rawBinNumber: the number of small bins to create. These small bins are grouped
             together by the algorithm to form the eventual buckets.
minPtsToBucket: if there are less than this number of points, everything is put
               into a single bucket.
minRegBucketSize: the smallest bucket size generated by the bucketing algorithm
                in normal circumstances.
sigThreshold: significance threshold to determine whether two adjacent bins are
               combined into the same bucket. For data science use case, if the
               threshold is met, combination will stop even if max number of buckets
               is exceeded. For UI use case, the max number of buckets specified 
               will always be adhered to.
minNumBuckets: the minimum number of buckets to generate in usual cases. This
               always takes precedence over sigThreshold. Exceptions where min
               buckets are ignored include: too few data, too few distinctive values,
               too few values with sufficient counts.
maxNumBuckets: the maximum number of buckets to generate in usual cases. In the
               UI use case this is always adhered to. In the data science use case,
               combination step stops once number of buckets is within maximum,
               but if sigThreshold is met, it could stop even when remaining 
               number of buckets is more than maximum.
kurtThreshold: when kurtosis is above this threshold, the column is considered
               to have a fat tail, and the fat tail splitting function is called
numTailBuckets: number of buckets to split from the fat tail
tailSize: proportion of data points to put into the fat tail and split
sizeThreshold: lower threshold of low-lift buckets, when low-lift buckets are
               smaller than this, consider to combine them for UI use case.
               This is not applied to the fat tail split results.
liftThreshold: lower threshold of ratio of lifts between two neighboring buckets,
               when combining smaller buckets for UI. If one of them have size
               less than sizeThreshold and on one side the lift difference is 
               less than liftThreshold, merge the two buckets.
               This is not applied to the fat tail split results.
fixBoundaries: used fixed boundaries. This is introduced for employee and revenue
               columns which the customer often uses for segmentation.
returns:
bktStats - tuples in the format of (sample_count, event_count) for each bucket
bucketMins - min of values in each bucket
bucketMaxes - max of values in each bucket
    """
    minPtsToBucket = max(minPtsToBucket, minRegBucketSize * 2)
    # all values are the same, or list empty
    if len(set(xListToBucket)) == 0:
        bktStats, bucketMins, bucketMaxes = [], [], []
    # single uniqe value, or not enough data to bucket
    elif len(set(xListToBucket)) == 1 or len(xListToBucket) <= minPtsToBucket:
        bktStats = [(len(eventListToBucket), sum(eventListToBucket))]
        bucketMins, bucketMaxes = [min(xListToBucket)], [max(xListToBucket)]
    # bucket values
    else: 
        # sort x List and event list
        idxOrdered = orderedIndicies(xListToBucket)
        xListToBucket_ord = [xListToBucket[i] for i in idxOrdered]
        eventListToBucket_ord = [eventListToBucket[i] for i in idxOrdered]

        # determine if we should use linear or geometric buckets
        useGeometric = checkGeometricDistribution(xListToBucket_ord)
        
        # iterate over choices until we find one that satisfies shape criteria
        for rawBinNumber in range(maxNumBuckets, minNumBuckets - 1, -1):
            # generate specified bins with linear or geometric distribution
            binIndex = createBinsByValueRange(xListToBucket_ord, rawBinNumber, useGeometric, minRegBucketSize)
            # calculate sample counts and event counts in each bin
            bktStats = [getCountsTuple(eventListToBucket_ord[binIndex[b]:binIndex[b + 1]]) 
                        for b in range(len(binIndex) - 1)]
            liftShapeOK = checkLiftShape(bktStats)
            liftDiffOK = checkLiftDifference(bktStats, liftThreshold, totalCnt, sizeThreshold)
            eventDiffOK = checkEventDifference(bktStats, eventDiffThreshold)
            if liftShapeOK and liftDiffOK and eventDiffOK:
                break

        # generate buckets
        bucketMins = [xListToBucket_ord[binIndex[i]] for i in range(len(binIndex) - 1)]
        bucketMaxes = [xListToBucket_ord[binIndex[i + 1] - 1] for i in range(len(binIndex) - 1)]

    return bktStats, bucketMins, bucketMaxes

def checkLiftShape(bktStats):
    if len(bktStats) <= 3:
        return True
    lifts = [b[1] / b[0] for b in bktStats]
    leftSign = [sign(current - left) for current, left in zip(lifts[1:-1], lifts[:-2])]
    rightSign = [sign(current - right) for current, right in zip(lifts[1:-1], lifts[2:])]
    isMinMax = [1 if lsign == rsign else 0 for lsign, rsign in zip(leftSign, rightSign)]
    singleMinMax = sum(isMinMax) <= 1
    return singleMinMax

def checkLiftDifference(bktStats, liftThreshold, totalCnt, sizeThreshold):
    if len(bktStats) < 2:
        return True
    liftDiffs = [getLiftDiff(bktStats[b], bktStats[b + 1], liftThreshold) for b in range(len(bktStats) - 1)]
    smallLiftDiff = [ld < liftThreshold for ld in liftDiffs]
    smallBucketLeft = [b[0] < sizeThreshold * totalCnt for b in bktStats[:-1]]
    smallBucketRight = [b[0] < sizeThreshold * totalCnt for b in bktStats[1:]]
    liftFail = [t[0] and  (t[1] or t[2]) for t in zip(smallLiftDiff, smallBucketLeft, smallBucketRight)]
    liftDiffOK = not(any(liftFail))
    return liftDiffOK

def checkEventDifference(bktStats, eventDiffThreshold):
    if len(bktStats) < 2:
        return True
    eventDiffs = [getEventDiff(bktStats[b], bktStats[b + 1]) for b in range(len(bktStats) - 1)]
    eventDiffOK = min(eventDiffs) >= eventDiffThreshold
    return eventDiffOK

def checkGeometricDistribution(xListToBucket_ord):
    return kurtosis(xListToBucket_ord, fisher=False) >= 3
        
def addVariablesToOutput(bandsDict, eventVector):
    """
    Expands the output data structure by adding a few calculated variables that
    the avro file needs.
    """
    # global stats used for later calculation
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
    """
    Delete a bin from current bins, merge it with the bin on its right side
    and return remaining bins
    binIndex: list of boundaries carving out bins, len()=num_bins+1
    bktStats: tuples in the format of (sample_count, event_count) for each bucket
    indexToDel: the index of a bin to delete, by design can't delete the last bin
    Returns: new binIndex and bktStats
    """
    # remove deleted bin index
    binIndex.pop(indexToDel + 1)
    # update stats of adjacent buckets
    newBucketSize = bktStats[indexToDel][0] + bktStats[indexToDel + 1][0]
    newBucketEvents = bktStats[indexToDel][1] + bktStats[indexToDel + 1][1]
    bktStats[indexToDel] = (newBucketSize, newBucketEvents)
    # remove deleted bucket stats
    bktStats.pop(indexToDel + 1)
    return binIndex, bktStats

def combineBuckets(binIndex, bktStats, sigThreshold, minNumBuckets, maxNumBuckets, forUI):
    """
    Combine raw bins by statistical significance to form desired number of buckets
    binIndex: list of boundaries carving out bins, len()=num_bins+1
    bktStats: tuples in the format of (sample_count, event_count) for each bucket
    sigThreshold: significance threshold to determine whether two adjacent bins are
                   combined into the same bucket. For data science use case, if the
                   threshold is met, combination will stop even if max number of buckets
                   is exceeded. For UI use case, the max number of buckets specified 
                   will always be adhered to.
    minNumBuckets: the minimum number of buckets to generate in usual cases. This
                   always takes precedence over sigThreshold. Exceptions where min
                   buckets are ignored include: too few data, too few distinctive values,
                   too few values with sufficient counts.
    maxNumBuckets: the maximum number of buckets to generate in usual cases. In the
                   UI use case this is always adhered to. In the data science use case,
                   combination step stops once number of buckets is within maximum,
                   but if sigThreshold is met, it could stop even when remaining 
                   number of buckets is more than maximum.
    forUI: boolean indicating whether it's for the UI use case
    Returns: new binIndex and bktStats
    """
    # check current number of buckets
    currNumBuckets = len(bktStats)
    if currNumBuckets <= max(maxNumBuckets, 1):
        return binIndex, bktStats
        
    sigList = [getSig(bktStats[b], bktStats[b + 1]) for b in range(len(bktStats) - 1)]
    
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
                sigList[indexToDel - 1] = getSig(bktStats[indexToDel - 1], bktStats[indexToDel])
            # update next sig if any
            if indexToDel < len(sigList) - 1:
                sigList[indexToDel + 1] = getSig(bktStats[indexToDel], bktStats[indexToDel + 1])
            # remove deleted sig value
            sigList.pop(indexToDel)

            currNumBuckets = currNumBuckets - 1
            if currNumBuckets <= maxNumBuckets:
                break
    return binIndex, bktStats

def getLiftDiff(bkta, bktb, threshold):
    """
    Gets the ratio of lifts from two buckets.
    bkta: tuple in the format of (sample_count, event_count)
    bktb: same as bkta, symmetrical
    """
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
        return rateB / rateA        

def getEventDiff(bkta, bktb):
    """
    Calculate the # of positive events that need to be exchanged between two buckets
    to make their lift equal.
    bkta: tuple in the format of (sample_count, event_count)
    bktb: same as bkta, symmetrical
    """
    cntA, posA = bkta
    cntB, posB = bktb
    avgRate = (posA + posB) / (cntA + cntB)
    expPosA = avgRate * cntA
    expPosB = avgRate * cntB
    evtDiff = abs((posA - expPosA) - (posB - expPosB))
    return evtDiff

# make boundaries continuous
def makeBoundariesContinuous(mins, maxes):
    for idx in range(len(mins) - 1):
        if maxes[idx] is not None:
            maxes[idx] = mins[idx + 1]
    return mins, maxes

def calculateMutualInfoBinary(splCnts, posCnts):
    """
    Calculates mutual information from a list of buckets.
    splCnts: sample counts in each bucket
    poscnts: positive events in each bucket
    returns: mutual information in each bucket
    """
    totalLength, totalPositives = sum(splCnts), sum(posCnts)
    rate = [float(totalLength - totalPositives) / totalLength, float(totalPositives) / totalLength]
    def relative_dep(splCnt, posCnt):
        joint_prob = [float(splCnt - posCnt) / totalLength, float(posCnt) / totalLength]
        p_x = float(splCnt) / totalLength
        returnVal = 0.0
        for yi in range(2):
            returnVal += joint_prob[yi] * math.log(joint_prob[yi] / (p_x * rate[yi])) if joint_prob[yi] != 0.0 else 0.0
        return returnVal
    mi_components = [relative_dep(splCnt, posCnt) for splCnt, posCnt in zip(splCnts, posCnts)]
    return mi_components
    
def getBucketsAndStats(columnData, eventVector, rawBinNumber=200, minPtsToBucket=50,
                       minSpecialValRatio=0.02, minRegBucketSize=20, minRegBucketRatio=0.01,
                       sigThreshold=8, minNumBuckets=3, maxNumBuckets=7, forUI=False,
                       kurtThreshold=3, numTailBuckets=2, tailSize=0.05, sizeThreshold=0.05,
                       liftThreshold=1.8, fixBoundaries=False, eventDiffThreshold=10):
    """
Main function to get numeric buckets and associated metrics to write to avro in
data_profile.py
This is used for both data science internal and UI purposes.

For data science purpose, the buckets are more granular and there is no 
restriction on boundaries.
Example of the data science use case:
getBucketsAndStats(columnData, eventVector,  rawBinNumber = 200,
                       minPtsToBucket = 50, minSpecialValRatio = 0.02, minRegBucketSize = 20,
                       sigThreshold = 8, minNumBuckets = 3, maxNumBuckets = 7, forUI = False,
                       kurtThreshold=3, numTailBuckets=2, tailSize=0.03, sizeThreshold=0.05, 
                       liftThreshold=1.8, fixBoundaries=False)

For UI use, there are upper and lower limits on number of buckets. It also uses
more 'round' boundaries. For most columns it means keeping 2 sigificant digits.
For employee count and revenue columns, when fixBoundaries is set to True, it 
uses some pre-defined boundaries.
Example of the UI use case:
fixbound = colName in ["BusinessEstimatedAnnualSales", "BusinessEstimatedEmployees"]
        bandsDictUI = getBucketsAndStats(columnData, eventVector,  rawBinNumber = 200,
                       minPtsToBucket = 50, minSpecialValRatio = 0.02, minRegBucketSize = 20,
                       sigThreshold = 8, minNumBuckets = 3, maxNumBuckets = 7, forUI = False,
                       kurtThreshold=3, numTailBuckets=2, tailSize=0.03, sizeThreshold=0.05, 
                       liftThreshold=1.8, fixBoundaries=fixbound)

columnData: input data, a numeric list
eventVector: a list of equivalent length compromising of only 1s and 0s
rawBinNumber: the number of small bins to create. These small bins are grouped
             together by the algorithm to form the eventual buckets.
minPtsToBucket: if there are less than this number of points, everything is put
               into a single bucket.
minSpecialValRatio: ratio threshold for a 'special value' to have its own bucket.
                Specifically, it's used for 1 in the current use case. NAs will
                always be in a separate bucket. 0s are always in a separate bucket
                when they are the minimum values. The special value status of 1
                in the UI use case is slightly more complicated -- it's only a 
                special value if threshold is met, and there are no possible buckets
                stranding 1.
minRegBucketSize: the smallest bucket size generated by the bucketing algorithm
                in normal circumstances.
sigThreshold: significance threshold to determine whether two adjacent bins are
               combined into the same bucket. For data science use case, if the
               threshold is met, combination will stop even if max number of buckets
               is exceeded. For UI use case, the max number of buckets specified 
               will always be adhered to.
minNumBuckets: the minimum number of buckets to generate in usual cases. This
               always takes precedence over sigThreshold. Exceptions where min
               buckets are ignored include: too few data, too few distinctive values,
               too few values with sufficient counts.
maxNumBuckets: the maximum number of buckets to generate in usual cases. In the
               UI use case this is always adhered to. In the data science use case,
               combination step stops once number of buckets is within maximum,
               but if sigThreshold is met, it could stop even when remaining 
               number of buckets is more than maximum.
forUI: boolean indicating whether it's for the UI use case
kurtThreshold: when kurtosis is above this threshold, the column is considered
               to have a fat tail, and the fat tail splitting function is called
numTailBuckets: number of buckets to split from the fat tail
tailSize: proportion of data points to put into the fat tail and split
sizeThreshold: lower threshold of low-lift buckets, when low-lift buckets are
               smaller than this, consider to combine them for UI use case.
               This is not applied to the fat tail split results.
liftThreshold: lower threshold of ratio of lifts between two neighboring buckets,
               when combining smaller buckets for UI. If one of them have size
               less than sizeThreshold and on one side the lift difference is 
               less than liftThreshold, merge the two buckets.
               This is not applied to the fat tail split results.
fixBoundaries: used fixed boundaries. This is introduced for employee and revenue
               columns which the customer often uses for segmentation.
returns: a dictionary with everything we need to write to avro file    
   """
    totalCnt = len(columnData)
    # process special values
    specialValues = identifySpecialValues(columnData, eventVector, minSpecialValRatio, forUI)    
    xListToBucket, eventListToBucket, bandsDict = processSpecialValues(columnData, eventVector, specialValues)
    maxNumBuckets = maxNumBuckets - len(bandsDict["min"])
    minNumBuckets = minNumBuckets - len(bandsDict["min"])
    maxNumBuckets = max(1, maxNumBuckets)
    minNumBuckets = max(1, minNumBuckets)
    minRegBucketSize = max(minRegBucketSize, ceil(minRegBucketRatio * len(columnData)))
    # add regular buckets
    # use round edges
    if forUI:
        if fixBoundaries:
            maxValFixBound, xListToBucket = truncateToFixedBoundaries(xListToBucket)
        else:
            xListToBucket = [roundTo5(x) for x in xListToBucket]
        bktStats, bucketMins, bucketMaxes = createRegularBucketsForUI(xListToBucket,
                         eventListToBucket, totalCnt, minPtsToBucket, sigThreshold,
                         rawBinNumber, minRegBucketSize, minNumBuckets, maxNumBuckets,
                         kurtThreshold, tailSize, numTailBuckets, sizeThreshold,
                         liftThreshold, eventDiffThreshold)
        bandsDict = addBucketsToOutput(bandsDict
                                   , bucketMins
                                   , bucketMaxes
                                   , [x[0] for x in bktStats]
                                   , [x[1] for x in bktStats])
        # make boundaries continuous
        if fixBoundaries:
            bandsDict["max"][-1] = maxValFixBound
        bandsDict["min"], bandsDict["max"] = makeBoundariesContinuous(bandsDict["min"], bandsDict["max"])
        
    else:  # data science version for MI
        bktStats, bucketMins, bucketMaxes = createRegularBucketsForMI(xListToBucket,
                         eventListToBucket, totalCnt, minPtsToBucket, sigThreshold,
                         rawBinNumber, minRegBucketSize, minNumBuckets, maxNumBuckets)
        bandsDict = addBucketsToOutput(bandsDict
                                   , bucketMins
                                   , bucketMaxes
                                   , [x[0] for x in bktStats]
                                   , [x[1] for x in bktStats])
    # add additional variables such as uncertainty coefficient etc.
    bandsDict = addVariablesToOutput(bandsDict, eventVector)
    
    return bandsDict
