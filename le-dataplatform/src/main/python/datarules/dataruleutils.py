from math import sqrt
import pandas as pd

# Return Conversion Rate and Number of Positive Events
def calculateOverallConversionRate(dataFrameColumn):
    return (len(dataFrameColumn), calculateConversionRate(dataFrameColumn))

def calculateConversionRate(dataFrameColumn):
    zeroLabels = (dataFrameColumn == 0).sum()
    oneLabels = (dataFrameColumn == 1).sum()
    conversionRate = float(oneLabels) / (oneLabels + zeroLabels)
    return conversionRate

def isCategorical(columnType):
    return columnType == "categorical"

def isNumerical(columnType):
    return columnType == "numerical"

def calculateSignificanceOfSubPopulation(subPopulationCountAndConversionRate, overallCountAndConversionRate):
    subPopulationCount, subPopulationConversionRate = subPopulationCountAndConversionRate
    overallPopulationCount , overallConversionRate = overallCountAndConversionRate

    if  subPopulationCount + overallPopulationCount > 0:
        r = (subPopulationCount * subPopulationConversionRate + overallPopulationCount * overallConversionRate) / (subPopulationCount + overallPopulationCount)
        if r > 0:
            pValue = (subPopulationConversionRate - overallConversionRate) / sqrt(r * (1.0 - r) * (1.0 / subPopulationCount + 1.0 / overallPopulationCount))
            return pValue
        else:
            return 0.0
    else:
        return 0.0

def getGroupedConversionRate(dataFrameColumn, eventColumn, overallConversionRate):
    df = pd.concat([dataFrameColumn, eventColumn], axis=1)
    df.columns = ['A', 'B']
    df = df.groupby(['A'])
    groupedConversionRate = {}
    for k, g in df:
        countForKey = int(g['B'].count())
        conversionRateForKey = float(calculateConversionRate(g['B']))
        countAndConversionRateForKey = (countForKey, conversionRateForKey)
        groupedConversionRate[k] = [ countForKey, conversionRateForKey , calculateSignificanceOfSubPopulation(countAndConversionRateForKey, overallConversionRate) ]

    return groupedConversionRate

def findDomain(emailString):
    try:
        if '@' in emailString:
            position = emailString.index('@') + 1
            return emailString[position:]
        else:
            print emailString
            return ''
    except:
        return ''

def ffloat(x):
    try:
        return float(x)
    except:
        return float('nan')
