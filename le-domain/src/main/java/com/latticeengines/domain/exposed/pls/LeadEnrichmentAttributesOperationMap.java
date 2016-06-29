package com.latticeengines.domain.exposed.pls;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties
public class LeadEnrichmentAttributesOperationMap {
    private List<LeadEnrichmentAttribute> selectedAttributes;

    private List<LeadEnrichmentAttribute> deselectedAttributes;

    public List<LeadEnrichmentAttribute> getSelectedAttributes() {
        return selectedAttributes;
    }

    public void setSelectedAttributes(List<LeadEnrichmentAttribute> selectedAttributes) {
        this.selectedAttributes = selectedAttributes;
    }

    public List<LeadEnrichmentAttribute> getDeselectedAttributes() {
        return deselectedAttributes;
    }

    public void setDeselectedAttributes(List<LeadEnrichmentAttribute> deselectedAttributes) {
        this.deselectedAttributes = deselectedAttributes;
    }

}
