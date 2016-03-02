from collections import OrderedDict
import logging
import uuid

from leframework.codestyle import overrides
from leframework.model.state import State

class PredictorGenerator(State):

    def __init__(self):
        State.__init__(self, "PredictorGenerator")
        self.logger = logging.getLogger(name='predictorgenerator')

    @overrides(State)
    def execute(self):
        mediator = self.mediator
        predictors = []
        configMetadata = self.getConfigMetadata()
        for key, profileMetadata in mediator.metadata[0].iteritems():
            if key + "_1" in mediator.schema["targets"]:
                continue
            self.logger.info("Generating predictors for " + key)
            predictors.append(self.generatePredictors(key, profileMetadata, configMetadata))

        self.result = sorted(predictors, key = lambda x: x["UncertaintyCoefficient"], reverse = True)

        # Add Result to Mediator
        self.mediator.predictors = self.result

    def getConfigMetadata(self):
        result = dict()
        configMetadata = self.mediator.schema["config_metadata"]
        if configMetadata is not None:
            for element in configMetadata["Metadata"]:
                result[element["ColumnName"]] = element
        return result

    def generatePredictors(self, colname, profileMetadata, configMetadata):
        elements = []

        attrLevelUncertaintyCoeff = 0
        hasNotNoneUC = False
        for record in profileMetadata:
            self.logger.info(record)
            element = OrderedDict()
            element["CorrelationSign"] = 1 if record["lift"] > 1 else -1
            element["Count"] = record["count"]

            # Lift value
            if record["lift"] is not None:
                element["Lift"] = record["lift"]

            # Band values
            if record["Dtype"] == "BND":
                element["LowerInclusive"] = record["minV"]
            if record["Dtype"] == "BND":
                element["UpperExclusive"] = record["maxV"]

            # Name
            element["Name"] = str(uuid.uuid4())

            # Uncertainty coefficient
            if record["uncertaintyCoefficient"] is not None:
                element["UncertaintyCoefficient"] = record["uncertaintyCoefficient"]
                attrLevelUncertaintyCoeff += element["UncertaintyCoefficient"]
                hasNotNoneUC = True

            # Discrete value
            if record["Dtype"] == "BND":
                element["Values"] = []
            else:
                element["Values"] = [record["columnvalue"]]

            # Handle null buckets for both continuous and discrete
            if "continuousNullBucket" in record and record["continuousNullBucket"] == True:
                element["Values"] = [None]
            if "discreteNullBucket" in record and record["discreteNullBucket"] == True:
                element["Values"] = ["null"]

            element["IsVisible"] = True
            elements.append(element)

        predictor = OrderedDict()
        predictor["Elements"] = elements
        predictor["Name"] = colname

        if colname in configMetadata:
            metadata = configMetadata[colname]
            predictor["Tags"] = metadata["Tags"]
            predictor["DataType"] = metadata["DataType"]
            predictor["DisplayName"] = metadata["DisplayName"]
            predictor["Description"] = metadata["Description"]
            predictor["ApprovedUsage"] = metadata["ApprovedUsage"]
            predictor["FundamentalType"] = metadata["FundamentalType"]

            predictor["Category"] = None
            if metadata["Extensions"] is not None:
                for e in metadata["Extensions"]:
                    if e["Key"] == "Category":
                        predictor["Category"] = e["Value"]
                        break
        else:
            predictor["Tags"] = None
            predictor["DataType"] = None
            predictor["DisplayName"] = None
            predictor["Description"] = None
            predictor["ApprovedUsage"] = None
            predictor["FundamentalType"] = None
            predictor["Category"] = None

        if hasNotNoneUC:
            predictor["UncertaintyCoefficient"] = attrLevelUncertaintyCoeff
        else:
            predictor["UncertaintyCoefficient"] = -1

        return predictor
