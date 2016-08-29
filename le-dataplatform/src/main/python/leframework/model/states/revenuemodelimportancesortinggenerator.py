import logging

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State

class RevenueModelImportanceSortingGenerator(State, JsonGenBase):

    def __init__(self):
        State.__init__(self, "ImportanceSortingGenerator")
        self.logger = logging.getLogger(name='importancesortinggenerator')
        self.importancesorting = {}
        self.nameListDict = {"DataCloud":["Ext_LEAccount"],
                  "TimeSeries":["TS_Analytic"],
                  "Span":["Product", "Span"],
                  "Revenue":["Product", "Revenue"],
                  "RollingRevenue": ["Product", "Revenue", "Rolling", "Sum"],
                  "Momentum":["Product", "Revenue", "Momentum"],
                  "Units":["Product", "Units"]}

    @overrides(State)
    def execute(self):
        try:
            self.logger.info("Calculating ImportanceSorting in ImportanceSortingGenerator")
            mediator = self.mediator
            schema = mediator.schema
            event = mediator.data[schema["target"]]
            features = mediator.data[schema["features"]]

            # Check if EV model
            if mediator.revenueColumn != None:
                self.importancesorting = self.calculateImportanceSorting(features, event)
                mediator.importancesorting = self.importancesorting
        except Exception as e:
            self.logger.error("Caught error in ModelQualityGenerator")
            self.logger.error(e)
            mediator.importancesorting = None

    # helper function to avoid division by zero
    def average(self, x):
        if len(x) == 0: return 0.0
        return float(sum(x)) / len(x)

    # bucket function that creates bucket by index
    def bucketFunction(self, event, val, nbuckets=20):
        ix = sorted(range(len(val)), key=lambda j: val[j], reverse=True)
        step = int(len(val)) / float(nbuckets + 1)
        ivals = [[i * step, (i + 1) * step - 1] for i in range(nbuckets)]
        ivals[nbuckets - 1][1] = len(val) - 1
        perc = [(x[1] - x[0] + 1.0) / len(val) for x in ivals]
        events = [sum([event[ix[i]] for i in range(int(x[0]), int(x[1]) + 1)]) for x in ivals]
        return perc, events

    def rankval(self, event, val, samplePercent=0.1):
        ixNA = [i for i, x in enumerate(val) if x is None]
        ix = [i for i, x in enumerate(val) if x is not None]
        if len(ixNA) == len(val):
            return 0.0, self.average(event), 1.0
        if len(ixNA) > 0:
            eventNA = sum([event[i] for i in ixNA])
        else:
            eventNA = 0.0
        percNA = len(ixNA) / float(len(val))
        perc, events = self.bucketFunction(event, val)
        perc.append(percNA)
        events.append(eventNA)
        def modRate(a, b):
            if b == 0: return 0.0
            return float(a) / b
        rate = [modRate(events[i], perc[i]) for i in range(len(perc))]
        iy = sorted(range(len(rate)), key=lambda j: rate[j], reverse=True)
        iret = -1
        cumsum = 0
        for i in range(0, len(iy)):
            cumsum = cumsum + perc[iy[i]]
            if cumsum > samplePercent:
                iret = i
                break
        eventsInRange = sum([events[iy[i]] for i in range(iret + 1)])
        totalEvents = sum(events)
        sizeOfRange = sum([perc[iy[i]] for i in range(iret + 1)])
        liftRate = eventsInRange / (totalEvents * sizeOfRange)
        liftNA = modRate(eventNA / totalEvents, percNA)
        return liftRate, liftNA, percNA

    def dictBucketingByName(self, nameList):
        return {key:[x for x in nameList if all(str.lower(str(u)) in str.lower(str(x)) for u in self.nameListDict[key])] for key in self.nameListDict.keys()}

    def dictBucketingByIndex(self, nameList):
        returnDict = self.nameListBucketingByName(nameList)
        mapDict = {x:i for i, x in enumerate(nameList)}
        return {key:[mapDict[x] for x in returnDict[key]] for key in returnDict.keys()}

    def calculateImportanceSorting(self, features, event):
        featuresDict = self.dictBucketingByName(features.columns)
        rankDict = {}
        for featureName in features.columns:
            col = features[featureName].tolist()
            rankVal = self.rankval(event, col)
            rankDict[featureName] = rankVal

        importanceSorting = {}
        for bucketName, _ in featuresDict.iteritems():
            importanceSorting[bucketName] = {}
            for feature in featuresDict[bucketName]:
                importanceSorting[bucketName][feature] = rankDict[feature]

        return importanceSorting

    @overrides(JsonGenBase)
    def getKey(self):
        return "ImportanceSorting"

    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.importancesorting
