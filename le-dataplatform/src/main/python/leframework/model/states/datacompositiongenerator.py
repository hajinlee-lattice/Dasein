from collections import OrderedDict
import logging

from leframework.codestyle import overrides
from leframework.model.state import State


class DataCompositionGenerator(State):
    def __init__(self):
        State.__init__(self, "DataCompositionGenerator")
        self._logger = logging.getLogger(name="DataCompositionGenerator")

    @overrides(State)
    def execute(self):
        structure = OrderedDict()
        schema = self.mediator.schema
        configMetadata = schema["config_metadata"]["Metadata"] if schema["config_metadata"] is not None else None
        if configMetadata is not None:
            # TODO Need to handle derived fields.
            fields = OrderedDict(self.__get_fields(configMetadata))
            transforms = list(self.__get_transforms(fields))

            structure["fields"] = fields
            structure["transforms"] = transforms

        self.getMediator().data_composition = structure

    def __get_fields(self, configMetadata):
        schema = self.mediator.schema
        result = []
        for name, dataType in schema["fields"].iteritems():
            metadata = [x for x in configMetadata if x["ColumnName"] == name]
            if len(metadata) == 0:
                continue
            metadata = metadata[0]

            details = OrderedDict()
            details["source"] = "REQUEST"
            if "DataSource" in metadata and metadata["DataSource"] is not None:
                if "DerivedColumns" in metadata["DataSource"]:
                    details["source"] = "PROPRIETARY"

            # TODO Decide if we need to handle TEMPORAL types or not.
            if dataType == "boolean":
                details["type"] = "BOOLEAN"
            elif dataType == "int" or dataType == "long":
                details["type"] = "INTEGER"
            elif dataType == "float" or dataType == "double":
                details["type"] = "FLOAT"
            else:
                details["type"] = "STRING"

            # TODO Should identify interesting non-feature columns through metadata.
            if name in schema["features"]:
                details["interpretation"] = "FEATURE"
            elif name == "Email":
                details["interpretation"] = "EMAIL_ADDRESS"
            elif name == "LeadID" or name == "ExternalId":
                details["interpretation"] = "RECORD_ID"
            elif name == "CompanyName":
                details["interpretation"] = "COMPANY_NAME"
            elif name == "City":
                details["interpretation"] = "COMPANY_CITY"
            elif name == "State":
                details["interpretation"] = "COMPANY_STATE"
            elif name == "Country":
                details["interpretation"] = "COMPANY_COUNTRY"
            elif name == "Website":
                details["interpretation"] = "WEBSITE"
            else:
                continue

            result.append((name, details))

        return result

    def __get_transforms(self, fields):
        pipeline = self.getMediator().pipeline.getPipeline()
        result = []
        for step in pipeline:
            columns = step.getOutputColumns()
            rtsModule = step.getRTSMainModule()

            for column in columns:
                name = column[0]["name"]

                if step.doColumnCheck() and (name not in fields or fields[name]["interpretation"] != "FEATURE"):
                    continue
                if len(column[1]) == 1:
                    result.append(self.__make_transform(rtsModule, name, column[0]["type"], [("column", k) for k in column[1]]))
                else:
                    l = [("column%d" % i, column[1][i-1]) for i in xrange(1, len(column[1])+1)]
                    result.append(self.__make_transform(rtsModule, name, column[0]["type"], l))
        return result

    def __make_transform(self, name, output, data_type, arguments):
        result = OrderedDict()
        result["name"] = name
        result["output"] = output
        result["type"] = data_type
        result["arguments"] = OrderedDict(arguments)
        return result

