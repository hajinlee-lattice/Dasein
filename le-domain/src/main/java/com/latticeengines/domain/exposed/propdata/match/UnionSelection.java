package com.latticeengines.domain.exposed.propdata.match;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.Predefined;

public class UnionSelection {

    @JsonProperty("PredefinedSelections")
    private Map<Predefined, String> predefinedSelections;

    @JsonProperty("CustomSelection")
    private ColumnSelection customSelection;

    public Map<Predefined, String> getPredefinedSelections() {
        return predefinedSelections;
    }

    public void setPredefinedSelections(Map<Predefined, String> predefinedSelections) {
        this.predefinedSelections = predefinedSelections;
    }

    public ColumnSelection getCustomSelection() {
        return customSelection;
    }

    public void setCustomSelection(ColumnSelection customSelection) {
        this.customSelection = customSelection;
    }
}
