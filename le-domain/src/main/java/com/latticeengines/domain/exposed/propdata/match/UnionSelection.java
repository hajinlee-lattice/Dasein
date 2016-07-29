package com.latticeengines.domain.exposed.propdata.match;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

public class UnionSelection {

    @JsonProperty("PredefinedSelections")
    private Map<ColumnSelection.Predefined, String> predefinedSelections;

    @JsonProperty("CustomSelection")
    private ColumnSelection customSelection;


    public Map<ColumnSelection.Predefined, String> getPredefinedSelections() {
        return predefinedSelections;
    }

    public void setPredefinedSelections(Map<ColumnSelection.Predefined, String> predefinedSelections) {
        this.predefinedSelections = predefinedSelections;
    }

    public ColumnSelection getCustomSelection() {
        return customSelection;
    }

    public void setCustomSelection(ColumnSelection customSelection) {
        this.customSelection = customSelection;
    }
}
