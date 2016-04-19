import logging
from collections import OrderedDict
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
            if "Tags" in metadata and metadata["Tags"] is not None:
                if "External" in metadata["Tags"]:
                    details["source"] = "PROPRIETARY"
                elif "Internal" in metadata["Tags"]:
                    details["source"] = "TRANSFORMS"

            # TODO Decide if we need to handle TEMPORAL types or not.
            if dataType == "boolean":
                details["type"] = "BOOLEAN"
            elif dataType == "int":
                details["type"] = "INTEGER"
            elif dataType == "long":
                details["type"] = "LONG"
            elif dataType == "float" or dataType == "double":
                details["type"] = "FLOAT"
            else:
                details["type"] = "STRING"

            # Keep in sync with FieldInterpretation
            if name in schema["features"]:
                details["interpretation"] = "Feature"
            elif name == "Id" or name == "LeadID" or name == "ExternalId":
                details["interpretation"] = "Id"
            elif name == "Event":
                details["interpretation"] = "Event"
            elif name == "Domain":
                details["interpretation"] = "Domain"
            elif name == "LastModifiedDate":
                details["interpretation"] = "Date"
            elif name == "CreatedDate":
                details["interpretation"] = "Date"
            elif name == "FirstName":
                details["interpretation"] = "FirstName"
            elif name == "LastName":
                details["interpretation"] = "LastName"
            elif name == "Title":
                details["interpretation"] = "Title"
            elif name == "Email":
                details["interpretation"] = "Email"
            elif name == "City":
                details["interpretation"] = "City"
            elif name == "State":
                details["interpretation"] = "State"
            elif name == "PostalCode":
                details["interpretation"] = "PostalCode"
            elif name == "Country":
                details["interpretation"] = "Country"
            elif name == "PhoneNumber":
                details["interpretation"] = "PhoneNumber"
            elif name == "Website":
                details["interpretation"] = "Website"
            elif name == "CompanyName":
                details["interpretation"] = "CompanyName"
            elif name == "Industry":
                details["interpretation"] = "Industry"
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

                if step.doColumnCheck() and (name not in fields or fields[name]["interpretation"] != "Feature"):
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

