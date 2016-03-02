package com.latticeengines.propdata.match.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.propdata.match.NameLocation;
import com.latticeengines.domain.exposed.propdata.match.OutputRecord;

class InternalOutputRecord extends OutputRecord {

    private String parsedDomain;
    private NameLocation parsedNameLocation;
    private Map<String, Map<String, Object>> resultsInSource = new HashMap<>();
    private List<Boolean> columnMatched;

    String getParsedDomain() {
        return parsedDomain;
    }

    void setParsedDomain(String parsedDomain) {
        this.parsedDomain = parsedDomain;
    }

    public NameLocation getParsedNameLocation() {
        return parsedNameLocation;
    }

    public void setParsedNameLocation(NameLocation parsedNameLocation) {
        this.parsedNameLocation = parsedNameLocation;
    }

    public Map<String, Map<String, Object>> getResultsInSource() {
        return resultsInSource;
    }

    public void setResultsInSource(Map<String, Map<String, Object>> resultsInSource) {
        this.resultsInSource = resultsInSource;
    }

    public List<Boolean> getColumnMatched() {
        return columnMatched;
    }

    public void setColumnMatched(List<Boolean> columnMatched) {
        this.columnMatched = columnMatched;
    }
}
