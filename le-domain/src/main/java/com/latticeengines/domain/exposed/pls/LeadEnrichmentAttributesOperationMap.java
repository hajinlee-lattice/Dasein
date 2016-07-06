package com.latticeengines.domain.exposed.pls;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties
public class LeadEnrichmentAttributesOperationMap {
    private List<String> selectedAttributes;

    private List<String> deselectedAttributes;

    public List<String> getSelectedAttributes() {
        return selectedAttributes;
    }

    public void setSelectedAttributes(List<String> selectedAttributes) {
        this.selectedAttributes = selectedAttributes;
    }

    public List<String> getDeselectedAttributes() {
        return deselectedAttributes;
    }

    public void setDeselectedAttributes(List<String> deselectedAttributes) {
        this.deselectedAttributes = deselectedAttributes;
    }

}
