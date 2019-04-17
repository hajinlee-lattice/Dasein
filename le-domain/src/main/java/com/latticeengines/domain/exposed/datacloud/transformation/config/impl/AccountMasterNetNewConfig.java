package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;
import java.util.Map;

public class AccountMasterNetNewConfig extends TransformerConfig {
    private Map<String, List<String>> filterCriteria;

    private List<String> groupBy;

    private List<String> resultFields;

    public Map<String, List<String>> getFilterCriteria() {
        return filterCriteria;
    }

    public void setFilterCriteria(Map<String, List<String>> filterCriteria) {
        this.filterCriteria = filterCriteria;
    }

    public List<String> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(List<String> groupBy) {
        this.groupBy = groupBy;
    }

    public List<String> getResultFields() {
        return resultFields;
    }

    public void setResultFields(List<String> resultFields) {
        this.resultFields = resultFields;
    }
}
