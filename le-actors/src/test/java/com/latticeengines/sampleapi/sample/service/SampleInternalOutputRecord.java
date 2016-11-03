package com.latticeengines.sampleapi.sample.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SampleInternalOutputRecord extends SampleOutputRecord {

    private String parsedDomain;
    private Boolean isPublicDomain = false;
    private String parsedDuns;
    private SampleNameLocation parsedNameLocation;
    private Map<String, Map<String, Object>> resultsInPartition = new HashMap<>();
    private Map<String, Object> queryResult = new HashMap<>();
    private List<Boolean> columnMatched;
    private Boolean failed = false;
    private String latticeAccountId;

    public String getParsedDomain() {
        return parsedDomain;
    }

    public void setParsedDomain(String parsedDomain) {
        this.parsedDomain = parsedDomain;
    }

    public Boolean isPublicDomain() {
        return isPublicDomain;
    }

    public void setPublicDomain(Boolean isPublicDomain) {
        this.isPublicDomain = isPublicDomain;
    }

    public String getParsedDuns() {
        return parsedDuns;
    }

    public void setParsedDuns(String parsedDuns) {
        this.parsedDuns = parsedDuns;
    }

    public SampleNameLocation getParsedNameLocation() {
        return parsedNameLocation;
    }

    public void setParsedNameLocation(SampleNameLocation parsedNameLocation) {
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

    public String getLatticeAccountId() {
        return latticeAccountId;
    }

    public void setLatticeAccountId(String latticeAccountId) {
        this.latticeAccountId = latticeAccountId;
    }
}
