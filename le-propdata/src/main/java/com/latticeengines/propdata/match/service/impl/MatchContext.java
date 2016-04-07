package com.latticeengines.propdata.match.service.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricFieldGroup;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.common.exposed.metric.annotation.MetricTagGroup;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.NameLocation;

public class MatchContext implements Fact, Dimension {

    private Set<String> domains;
    private Set<NameLocation> nameLocations;
    private MatchInput input;
    private MatchOutput output;
    private Map<String, List<String>> sourceColumnsMap;
    private Map<String, List<String>> columnPriorityMap;
    private Map<String, List<Map<String, Object>>> resultsBySource;
    private List<InternalOutputRecord> internalResults;
    private boolean returnUnmatched;
    private Long numRows;
    private MatchEngine matchEngine;

    @MetricFieldGroup(excludes = { "InputRows" })
    @MetricTagGroup(excludes = { "MatchEngine" })
    public MatchInput getInput() {
        return input;
    }

    public void setInput(MatchInput input) {
        this.input = input;
    }

    public Set<String> getDomains() {
        return domains;
    }

    public void setDomains(Set<String> domains) {
        this.domains = domains;
    }

    public Set<NameLocation> getNameLocations() {
        return nameLocations;
    }

    public void setNameLocations(Set<NameLocation> nameLocations) {
        this.nameLocations = nameLocations;
    }

    @MetricFieldGroup
    @MetricTagGroup
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

    public Map<String, List<String>> getColumnPriorityMap() {
        return columnPriorityMap;
    }

    public void setColumnPriorityMap(Map<String, List<String>> columnPriorityMap) {
        this.columnPriorityMap = columnPriorityMap;
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

    public Long getNumRows() {
        return numRows;
    }

    public void setNumRows(Long numRows) {
        this.numRows = numRows;
    }

    public MatchEngine getMatchEngine() {
        return matchEngine;
    }

    public void setMatchEngine(MatchEngine matchEngine) {
        this.matchEngine = matchEngine;
    }

    @MetricTag(tag = "MatchEngine")
    public String getMatchEngineAsString() {
        return matchEngine.getName();
    }

    public enum MatchEngine {
        REAL_TIME("RealTime"), BULK("Bulk");
        private String name;

        MatchEngine(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

    }
}
