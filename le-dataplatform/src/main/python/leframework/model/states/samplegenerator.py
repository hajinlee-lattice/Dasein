from collections import OrderedDict
import logging
import pandas as pd

from leframework.codestyle import overrides
from leframework.model.state import State
from leframework.util.reservedfieldutil import ReservedFieldUtil

class SampleGenerator(State):

    def __init__(self):
        State.__init__(self, "SampleGenerator")
        self.logger = logging.getLogger(name='samplegenerator')

    @overrides(State)
    def execute(self):
        preTransform = self.mediator.allDataPreTransform.copy(deep=False)

        # Sort PreTransform
        scoreColumnName = self.mediator.schema["reserved"]["score"]
        preTransform.sort(scoreColumnName, axis=0, ascending=False, inplace=True)

        # Generate Samples
        readoutSample = self.generateReadoutSample(preTransform, scoreColumnName)
        (topSample, bottomSample) = self.generateTopAndBottomSamples(readoutSample)

        # Clean Reserved Fields
        self.cleanReservedFields(readoutSample)

        # Add Results to Mediator
        self.mediator.readoutsample = readoutSample
        self.mediator.topsample = topSample
        self.mediator.bottomsample = bottomSample

    def generateReadoutSample(self, preTransform, scoreColumnName):
        rows = preTransform.shape[0]
        # Extract Rows
        if rows > 2000:
            result = preTransform[:1000]
            result = result.append(preTransform[rows - 1000:])
        else:
            result = preTransform

        # Map Scores
        percentileScores = []
        if rows > 2000:
            percentileScores = map(lambda e: self.percentile(e, top = True), result[scoreColumnName][:1000].as_matrix()) + \
                               map(lambda e: self.percentile(e, top = False), result[scoreColumnName][1000:].as_matrix())
        else:
            percentileScores = map(lambda e: self.percentile(e), result[scoreColumnName].as_matrix())

        # Update PercentileScores
        percentileScores = pd.Series(percentileScores, index=result.index)
        result[self.mediator.schema["reserved"]["percentilescore"]].update(percentileScores)

        # Map Targets
        converted = map(lambda e: "Y" if e > 0 else "N", result[self.mediator.schema["target"]].as_matrix())

        # Update Converted
        converted = pd.Series(converted, index=result.index)
        result[self.mediator.schema["reserved"]["converted"]].update(converted)

        return result

    def percentile(self, score, top = True):
        buckets = self.mediator.percentileBuckets
        maxScore = buckets[0]["MaximumScore"] if len(buckets) != 0 else 1

        if score >= maxScore:
            return 100
        else:
            order = 1 if top else -1
            for bucket in buckets[::order]:
                if score >= bucket["MinimumScore"] and score < bucket["MaximumScore"]:
                    return bucket["Percentile"]

        return None

    def cleanReservedFields(self, dataFrame):
        columns = dataFrame.columns.tolist()
        reserved = self.mediator.schema["reserved"]

        for key in reserved.keys():
            index = columns.index(reserved[key])
            columns[index] = ReservedFieldUtil.extractDisplayName(columns[index])

        dataFrame.columns = columns

    def generateTopAndBottomSamples(self, readoutSample):
        mediator = self.mediator
        schema = mediator.schema
        samples = schema["samples"]

        def getTestSample(row, requireValue):
            if not isTest(row):
                return None
            lead = OrderedDict()
            for colname, samplename in {'CompanyName':'Company', 'FirstName':'FirstName', 'LastName':'LastName'}.iteritems():
                if colname not in samples.values():
                    lead[samplename] = '(Not Provided)'
                else:
                    ## If the column exists in the sample and is required, but the value is not populated, reject this lead
                    if requireValue and pd.isnull(row[colname]):
                        return None
                    lead[samplename] = row[colname].strip() if pd.notnull(row[colname]) else '(Unknown)'
            lead["Converted"] = True if row[schema["target"]] == 1 else False
            lead["Score"] = row[schema["reserved"]["percentilescore"]]
            return lead

        def isSpam(row):
            spamIndicator = row[samples["spamindicator"]] if "spamindicator" in samples else None
            return True if spamIndicator is not None and spamIndicator == 1 else False

        def isTest(row):
            return not row[schema["reserved"]["training"]]

        def generateTopSample():
            result = []
            converted = 7; notConverted = 3
            sampleSize = converted + notConverted
            numConverted = 0
            numNotConverted = 0
            companies = set()
            rows = readoutSample.shape[0]
            for i in xrange(rows):
                row = readoutSample.iloc[i]
                sample = getTestSample(row, requireValue=True)
                if sample is not None and not isSpam(row):
                    company = sample["Company"]
                    if "CompanyName" not in samples.values() or company not in companies:
                        leadConverted = sample["Converted"]
                        if leadConverted and numConverted < converted:
                            result.append(sample)
                            companies.add(company)
                            numConverted += 1
                        elif not leadConverted and numNotConverted < notConverted:
                            result.append(sample)
                            companies.add(company)
                            numNotConverted += 1
                if len(result) == sampleSize: break
            return result

        def generateBottomSample():
            result = []
            sampleSize = 10
            numSample = 0
            companies = set()
            rows = readoutSample.shape[0]
            for i in xrange(rows):
                row = readoutSample.iloc[rows - 1 - i]
                sample = getTestSample(row, requireValue=False)
                if sample is not None:
                    company = sample["Company"]
                    if "CompanyName" not in samples.values() or company not in companies:
                        leadConverted = sample["Converted"]
                        if not leadConverted:
                            result.append(sample)
                            companies.add(company)
                            numSample += 1
                if len(result) == sampleSize: break
            result.reverse()
            return result

        return generateTopSample(), generateBottomSample()
