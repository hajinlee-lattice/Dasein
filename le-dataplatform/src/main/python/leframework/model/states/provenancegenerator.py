import logging
from collections import OrderedDict
from leframework.codestyle import overrides
from leframework.model.state import State


class ProvenanceGenerator(State):

    def __init__(self):
        State.__init__(self, "ProvenanceGenerator")
        self.logger = logging.getLogger(name='provenancegenerator')

    @overrides(State)
    def execute(self):
        properties = self.mediator.provenanceProperties

        if len(properties) > 0:
            self.result = OrderedDict()
            if "DataLoader_Instance" in properties.keys():
                self.result["SourceURL"] = properties["DataLoader_Instance"]
            if "DataLoader_TenantName" in properties.keys():
                self.result["TenantName"] = properties["DataLoader_TenantName"]
            if "DataLoader_Query" in properties.keys():
                self.result["QueryName"] = properties["DataLoader_Query"]
            if "Event_Table_Name" in properties.keys():
                self.result["EventTableName"] = properties["Event_Table_Name"]
            if "Source_Schema_Interpretation" in properties.keys():
                self.result["SourceSchemaInterpretation"] = properties["Source_Schema_Interpretation"]
            if "Training_Table_Name" in properties.keys():
                self.result["TrainingTableName"] = properties["Training_Table_Name"]
            if "Predefined_ColumnSelection_Name" in properties.keys():
                self.result["Predefined_ColumnSelection_Name"] = properties["Predefined_ColumnSelection_Name"]
                self.result["Predefined_ColumnSelection_Version"] = properties["Predefined_ColumnSelection_Version"]
            elif "Customized_ColumnSelection" in properties.keys():
                self.result["Customized_ColumnSelection"] = properties["Customized_ColumnSelection"]

            for propertyName in properties:
                self.result[propertyName] = properties[propertyName]
            if "Data_Cloud_Version" in properties.keys():
                self.result["Data_Cloud_Version"] = properties["Data_Cloud_Version"]
        else:
            self.logger.error("Provenance property is null.")
            self.result = OrderedDict()

        # Add Result to Mediator
        self.mediator.eventtableprovenance = self.result
