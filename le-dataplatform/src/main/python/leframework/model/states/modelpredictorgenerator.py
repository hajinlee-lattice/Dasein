import collections, itertools, json, logging, operator

from leframework.codestyle import overrides
from leframework.model.state import State
from leframework.model.states.averageprobabilitygenerator import AverageProbabilityGenerator
from leframework.model.states.summarygenerator import SummaryGenerator

class ModelPredictorGenerator(State):

    approvedUsagesToDisplay = set([u'Model', u'ModelAndModelInsights', u'ModelAndAllInsights'])

    def __init__(self):

        super(ModelPredictorGenerator, self).__init__('ModelPredictorGenerator')
        self.logger = logging.getLogger(name='modelpredictorgenerator')
        self.modelpredictor = collections.OrderedDict()

    @overrides(State)
    def execute(self):

        mediator = self.getMediator()
        modeldetail = {}
        states = self.getStateMachine().getStates()

        for state in states:
            if isinstance(state, AverageProbabilityGenerator):
                modeldetail['AverageProbability'] = state.getJsonProperty()
            if isinstance(state, SummaryGenerator):
                modeldetail['Summary'] = state.getJsonProperty()

        try:
            allColumnMetadata = mediator.schema['config_metadata']['Metadata']
        except:
            allColumnMetadata = {}

        columnMetadata = {}
        for metadata in allColumnMetadata:
            columnMetadata[metadata['ColumnName']] = metadata

        if modeldetail["AverageProbability"] is None:
            self.error('No AverageProbability in the model')
            return

        averageProb = modeldetail["AverageProbability"]

        if modeldetail["Summary"] is None:
            self.error('No ModelSummary information in the model')
            return

        if modeldetail["Summary"]["Predictors"] is None:
            self.error('No Predictors information in the model')
            return

        modelPredictorColNames = [ \
                'Original Column Name', \
                'Attribute Name', \
                'Category', \
                'FundamentalType', \
                'Predictive Power', \
                'Attribute Value', \
                'Conversion Rate', \
                'Lift', \
                'Total Leads', \
                'Frequency(#)', \
                'Frequency/Total Leads', \
                'ApprovedUsage', \
                'Tags', \
                'StatisticalType', \
                'Attribute Description', \
                'DataSource' ]
        modelPredictorRows = []

        nameArray = []
        sumArray = []

        # This section calculates the total #leads for each predictor. Ideally it should be the same for each predictor,
        # but there seem to be some exceptions.
        # dictArray is a list of dictionaries with attributeName = #Total Leads
        
        siddata = sorted(modeldetail["Summary"]["Predictors"], key=operator.itemgetter('Name'))
        sidgroups = itertools.groupby(siddata, operator.itemgetter('Name'))

        for key, group in sidgroups: 
            for value in group:
                total = 0
                for i in value['Elements']:
                    total = total + i['Count']
                nameArray.append(value['Name'])
                sumArray.append(total)
        dictArray = dict(zip(nameArray,sumArray))

        for predictor in modeldetail["Summary"]["Predictors"]:
            if not predictor["ApprovedUsage"] or len(self.approvedUsagesToDisplay.intersection(predictor["ApprovedUsage"])) == 0:
                continue
            otherPredictorElements = []
            for predictorElement in predictor["Elements"]:
                if self._isMergeWithOther(predictor, predictorElement, dictArray):
                    otherPredictorElements.append(predictorElement)
                    continue
                modelPredictorRows.append(self._generatePredictorElement(len, averageProb, \
                        dictArray, predictor, predictorElement, columnMetadata))
                
            if (len(otherPredictorElements) > 0):
                mergedPredictorElement = self._mergePredictorElements(otherPredictorElements, averageProb)
                modelPredictorRows.append(self._generatePredictorElement(len, averageProb, \
                        dictArray, predictor, mergedPredictorElement, columnMetadata))

        modelPredictor = {'colnames':modelPredictorColNames, 'rows':tuple(modelPredictorRows)}

        mediator.modelPredictor = modelPredictor

    def _isMergeWithOther(self, predictor, element, dictArray):
        length = 0
        if element["Values"] is not None:
            length = len(element["Values"])
        if (length == 0):
            return False
        if element["Values"][0] == "Other":
            return True
        if ("LowerInclusive" in element or "UpperExclusive" in element):
            return False
        if len(predictor["Elements"]) <=3:
            return False
        if "Count" in element:
            if element["Count"] is None:
                return True
            else:
                freq = element["Count"] / float(dictArray[predictor["Name"]])
                return True if freq < 0.01 else False
        return True

    def _mergePredictorElements(self, otherElements, averageProb):
        mergedElement = dict()
        mergedElement["Values"] = ["Other"]
        mergedCount = 0
        mergedLift = 0;
        for element in otherElements:
            leadCount = element["Count"]
            mergedCount += leadCount if leadCount is not None else 0
            if (element["Lift"] is not None and leadCount is not None):
                mergedLift += element["Lift"] * leadCount; 
            
        mergedElement["Count"] = mergedCount
        if (mergedCount != 0) :
            mergedElement["Lift"] = mergedLift / float(mergedCount)
        return mergedElement

    def _generatePredictorElement(self, len, averageProb, dictArray, predictor, predictorElement, columnNameToMetadataElement):

        columnName = unicode(predictor["Name"])
        attname = unicode(predictor["DisplayName"]) if predictor["DisplayName"] else u''
        category = unicode(predictor["Category"]) if predictor["Category"] else u''
        fundtype = unicode(predictor["FundamentalType"]) if predictor["FundamentalType"] else u''
        predpower = unicode(predictor["UncertaintyCoefficient"]) if predictor["UncertaintyCoefficient"] else u''

        attvalue = u''
        length = 0
        if predictorElement["Values"] is not None:
            length = len(predictorElement["Values"])
        if (length == 0): #This implies that value is of type bucket and not categorical
            if predictorElement["UpperExclusive"] is not None:
                attvalue = '< {}'.format(unicode(predictorElement["UpperExclusive"]))
            else:
                attvalue = '>= {}'.format(unicode(predictorElement["LowerInclusive"]))
        elif (predictorElement["Values"])[length - 1] is None:
            attvalue = 'Not Available'
        else:
            valueStr = '['
            for val in predictorElement["Values"]:
                if val is not None:
                    valueStr += '"'
                    newVal = self._mapBinaryValue(predictor, val)
                    valueStr += unicode(newVal)
                    valueStr += '"'
                    if val != (predictorElement["Values"])[length - 1]:
                        valueStr += ';'
            valueStr += ']'
            attvalue = valueStr


        convrate = unicode(averageProb * predictorElement["Lift"]) if 'Lift' in predictorElement else u''
        lift = unicode(predictorElement["Lift"]) if 'Lift' in predictorElement else u''
        totalleads = u''

        if predictor['Name'] in dictArray.keys():
            if dictArray[predictor['Name']] is None:
                totalleads = u'notFound'
            else:
                totalleads = unicode(dictArray[predictor['Name']])

        freq = u''
        freqovertotal = u''

        if 'Count' in predictorElement:
            if predictorElement["Count"] is None:
                freq = u'null'
                freqovertotal = u'null'
            else:
                freq = unicode(predictorElement["Count"])
                freqovertotal = unicode(predictorElement["Count"] / float(dictArray[predictor['Name']]))

        appusage = unicode(predictor["ApprovedUsage"]) if predictor["ApprovedUsage"] else u''

        tags = u''
        stattype = u''
        desc = u''
        datasource = u''

        if columnName in columnNameToMetadataElement:
            extraMetadataInformation = columnNameToMetadataElement[columnName]

            tags = unicode(extraMetadataInformation["Tags"])
            stattype = unicode(extraMetadataInformation["StatisticalType"])
            desc = unicode(extraMetadataInformation["Description"])
            datasource = unicode(extraMetadataInformation["DataSource"])

        return (columnName, attname, category, fundtype, predpower, attvalue, convrate, lift, totalleads, \
                freq, freqovertotal, appusage, tags, stattype, desc, datasource)


    def _mapBinaryValue(self, predictor, val):
        if not predictor["FundamentalType"]:
            return val
        fundamentalType = predictor["FundamentalType"].upper()
        if fundamentalType != "BOOLEAN":
            return val
        upperVal = str(val).upper()
        if upperVal in ["TRUE", "T", "YES", "Y", "C", "D", "1"]:
            return "Yes"
        if upperVal in ["FALSE", "F", "NO", "N", "0"]:
            return "No"
        if upperVal in ["", "NA", "N/A", "-1", "NULL", "NOT AVAILABLE"]:
            return "Not Available"
        return val