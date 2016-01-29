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

        if self.mediator.schema["config_metadata"] != None:
            # TODO Need to handle derived fields.
            fields = OrderedDict(self.__get_fields())
            transforms = list(self.__get_transforms(fields))

            structure["fields"] = fields
            structure["transforms"] = transforms

        self.getMediator().data_composition = structure
    
    def __get_fields(self):
        schema = self.getMediator().schema
        config_metadata = schema["config_metadata"]["Metadata"]
        
        result = list()
        for name, data_type in schema["fields"].iteritems():
            metadata = [x for x in config_metadata if x["ColumnName"] == name]
            if len(metadata) == 0:
                continue
            metadata = metadata[0]
            
            details = OrderedDict()
            details["source"] = "REQUEST"
            if metadata["DataSource"] is not None:
                if "DerivedColumns" in metadata["DataSource"]:
                    details["source"] = "PROPRIETARY"
            
            # TODO Decide if we need to handle TEMPORAL types or not.
            if data_type == "boolean":
                details["type"] = "BOOLEAN"
            elif data_type == "int" or data_type == "long":
                details["type"] = "INTEGER"
            elif data_type == "float" or data_type == "double":
                details["type"] = "FLOAT"
            else:
                details["type"] = "STRING"
            
            # TODO Should identify interesting non-feature columns through metadata.
            if name in schema["features"]:
                details["interpretation"] = "FEATURE"
            elif name == "Email":
                details["interpretation"] = "EMAIL_ADDRESS"
            elif name == "LeadID":
                details["interpretation"] = "RECORD_ID"
            else:
                continue

            result.append((name, details))

        return result

    def __get_transforms(self, fields):
        pipeline = self.getMediator().pipeline.getPipeline()
        result = list()
        for step in pipeline:
            columns = step.getOutputColumns()
            rtsModule = step.getRTSMainModule()
            
            for column in columns:
                name = column[0]["name"]
                if name not in fields or fields[name]["interpretation"] != "FEATURE":
                    continue
                result.append(self.__make_transform(
                    rtsModule, name, column[0]["type"], [("column", k) for k in column[1]]))

        return result
    
    def __make_transform(self, name, output, data_type, arguments):
        result = OrderedDict()
        result["name"] = name
        result["output"] = output
        result["type"] = data_type
        result["arguments"] = OrderedDict(arguments)
        return result
            
