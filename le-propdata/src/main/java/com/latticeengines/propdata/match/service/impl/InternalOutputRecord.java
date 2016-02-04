package com.latticeengines.propdata.match.service.impl;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.propdata.match.OutputRecord;

class InternalOutputRecord extends OutputRecord {

    private String parsedDomain;
    private Map<String, Map<String, Object>> resultsInSource = new HashMap<>();

    String getParsedDomain() {
        return parsedDomain;
    }

    void setParsedDomain(String parsedDomain) {
        this.parsedDomain = parsedDomain;
    }

    public Map<String, Map<String, Object>> getResultsInSource() {
        return resultsInSource;
    }

    public void setResultsInSource(Map<String, Map<String, Object>> resultsInSource) {
        this.resultsInSource = resultsInSource;
    }
}
