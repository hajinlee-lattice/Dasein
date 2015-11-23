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
            fields = OrderedDict(self._get_fields())
            transforms = list(self._get_transforms(fields))

            structure["fields"] = fields
            structure["transforms"] = transforms

        self.getMediator().data_composition = structure
    
    def _get_fields(self):
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

    def _get_transforms(self, fields):
        pipeline = self.getMediator().pipeline.getPipeline()
        # TODO Create a better mechanism for retrieving this step.
        step = next(x for x in pipeline if x.__class__.__name__ == "ImputationStep")
        imputations = step.enumMappings_
    
        result = list()
        for name, details in fields.iteritems():
            if details["interpretation"] != "FEATURE":
                continue
                
            if details["type"] == "STRING":
                result.append(self._make_transform(
                    "encode_string", name, "INTEGER", [("column", name)]))
            
            if details["type"] != "FLOAT":
                result.append(self._make_transform(
                    "make_float", name, "FLOAT", [("column", name)]))
            
            if name in imputations:
                result.append(self._make_transform(
                    "replace_null_value", name, "FLOAT",
                    [("column", name), ("value", imputations[name])]))

        return result
    
    def _make_transform(self, name, output, data_type, arguments):
        result = OrderedDict()
        result["name"] = name
        result["output"] = output
        result["type"] = data_type
        result["arguments"] = OrderedDict(arguments)
        return result
            
