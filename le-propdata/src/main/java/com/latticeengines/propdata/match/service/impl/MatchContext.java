package com.latticeengines.propdata.match.service.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;

public class MatchContext {

    private MatchStatus status;
    private Set<String> domains;
    private MatchOutput output;
    private Map<String, List<String>> sourceColumnsMap;
    private Map<String, List<Map<String, Object>>> resultsBySource;
    private List<InternalOutputRecord> internalResults;
    private boolean returnUnmatched;

    public MatchStatus getStatus() {
        return status;
    }

    public void setStatus(MatchStatus status) {
        this.status = status;
    }

    public Set<String> getDomains() {
        return domains;
    }

    public void setDomains(Set<String> domains) {
        this.domains = domains;
    }

    public MatchOutput getOutput() {
        return output;
    }

    public void setOutput(MatchOutput output) {
        this.output = output;
    }

    public Map<String, List<String>> getSourceColumnsMap() {
        return sourceColumnsMap;
    }

    public void setSourceColumnsMap(Map<String, List<String>> sourceColumnsMap) {
        this.sourceColumnsMap = sourceColumnsMap;
    }

    public Map<String, List<Map<String, Object>>> getResultsBySource() {
        return resultsBySource;
    }

    public void setResultsBySource(Map<String, List<Map<String, Object>>> resultsBySource) {
        this.resultsBySource = resultsBySource;
    }

    public List<InternalOutputRecord> getInternalResults() {
        return internalResults;
    }

    public void setInternalResults(List<InternalOutputRecord> internalResults) {
        this.internalResults = internalResults;
    }

    public boolean isReturnUnmatched() {
        return returnUnmatched;
    }

    public void setReturnUnmatched(boolean returnUnmatched) {
        this.returnUnmatched = returnUnmatched;
    }
}
