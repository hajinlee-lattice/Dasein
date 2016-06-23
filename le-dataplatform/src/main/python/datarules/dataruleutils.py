from math import sqrt
import re
import pandas as pd
from datetime import datetime, timedelta

# Return Conversion Rate and Number of Positive Events
def calculateOverallConversionRate(dataFrameColumn):
    return (len(dataFrameColumn), calculateConversionRate(dataFrameColumn))

def calculateConversionRate(dataFrameColumn):
    zeroLabels = (dataFrameColumn == 0).sum()
    oneLabels = (dataFrameColumn == 1).sum()
    if (oneLabels + zeroLabels) > 0:
        conversionRate = float(oneLabels) / (oneLabels + zeroLabels)
        return conversionRate
    else:
        return 0.0

def selectIdColumn(dataFrame):
    if "Id" in dataFrame.columns:
        return "Id"
    elif "LeadID" in dataFrame.columns:
        return "LeadID"
    elif "ExternalID" in dataFrame.columns:
        return "ExternalID"

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

def getDomainColumn(dataFrame):
    if "Domain" in dataFrame.columns:
        return normalizeDomain(dataFrame[u'Domain'])
    elif "Website" in dataFrame.columns:
        return normalizeDomain(dataFrame[u'Website'])
    elif "Email" in dataFrame.columns:
        return normalizeDomain(extractDomainFromEmail(dataFrame[u'Email']))

def cleanDomainInEmail(emailAsString):
    if emailAsString is None:
        return emailAsString
    elif '@' in emailAsString:
        return emailAsString[emailAsString.index('@')]
    else:
        return emailAsString

def cleanDomain(domainAsString):
    if domainAsString is None:
        return domainAsString
    else:
        if "http://" in domainAsString:
            domainAsString = re.sub("^http://", "", domainAsString)
        if "www." in domainAsString:
            domainAsString = re.sub("^www[.]", "", domainAsString)
        if "." in domainAsString:
            domainAsString = re.sub(".*$", "", domainAsString)
        if "NULL" in domainAsString:
            domainAsString = None
        return domainAsString.upper()

def addSortColumn(dataFrame, domainColumn, eventColumn, leadCreationDateColumn):
    dataFrame[u'sort'] = dataFrame.apply(applySort)
    dataFrame = dataFrame.sort([u'sort'])

    dataFrame = dataFrame.drop(u'sort', 1)

    return dataFrame

def applySort(dataFrameColumns):
    optimalCreationTime = (datetime.utcnow() - timedelta(days=45)).total_seconds() * 1000
    eventColumn = dataFrameColumns['target']
    leadCreationDate = pd.to_datetime(dataFrameColumns['LeadCreationDate']).total_seconds() * 1000

    if leadCreationDate is None:
        return None
    else:
        return eventColumn + (1.0 / abs(leadCreationDate - optimalCreationTime))

def extractDomainFromEmail(Domain):
    return Domain.apply(cleanDomainInEmail)

def normalizeDomain(Domain):
    return Domain.apply(cleanDomain)

