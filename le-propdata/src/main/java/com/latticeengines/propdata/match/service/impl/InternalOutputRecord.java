package com.latticeengines.propdata.match.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.propdata.match.NameLocation;
import com.latticeengines.domain.exposed.propdata.match.OutputRecord;

class InternalOutputRecord extends OutputRecord {

    private String parsedDomain;
    private NameLocation parsedNameLocation;
    private Map<String, Map<String, Object>> resultsInPartition = new HashMap<>();
    private Map<String, Object> queryResult = new HashMap<>();
    private List<Boolean> columnMatched;
    private Boolean failed = false;

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

    public Map<String, Map<String, Object>> getResultsInPartition() {
        return resultsInPartition;
    }

    public void setResultsInPartition(Map<String, Map<String, Object>> resultsInPartition) {
        this.resultsInPartition = resultsInPartition;
    }

    public Map<String, Object> getQueryResult() {
        return queryResult;
    }

    public void setQueryResult(Map<String, Object> queryResult) {
        this.queryResult = queryResult;
    }

    public List<Boolean> getColumnMatched() {
        return columnMatched;
    }

    public void setColumnMatched(List<Boolean> columnMatched) {
        this.columnMatched = columnMatched;
    }

    public Boolean isFailed() {
        return failed;
    }

    public void setFailed(Boolean failed) {
        this.failed = failed;
    }
}
