package com.latticeengines.datacloud.match.service.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import scala.concurrent.Future;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricFieldGroup;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.common.exposed.metric.annotation.MetricTagGroup;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

public class MatchContext implements Fact, Dimension {

    private Set<String> domains;
    private Set<NameLocation> nameLocations;
    private ColumnSelection columnSelection;
    private MatchInput input;
    private MatchOutput output;
    private Map<String, Set<String>> partitionColumnsMap;
    private Map<String, List<Map<String, Object>>> resultsByPartition;
    private List<Map<String, Object>> resultSet;
    private List<InternalOutputRecord> internalResults;
    private boolean seekingIdOnly;
    private boolean isCDLMatch;
    private DynamoDataUnit customAccountDataUnit;
    private MatchEngine matchEngine;

    @JsonIgnore
    private String contextId;

    @JsonIgnore
    private List<Future<Object>> futuresResult;

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

    public ColumnSelection getColumnSelection() {
        return columnSelection;
    }

    public void setColumnSelection(ColumnSelection columnSelection) {
        this.columnSelection = columnSelection;
    }

    @MetricFieldGroup
    @MetricTagGroup
    public MatchOutput getOutput() {
        return output;
    }

    public void setOutput(MatchOutput output) {
        this.output = output;
    }

    public Map<String, Set<String>> getPartitionColumnsMap() {
        return partitionColumnsMap;
    }

    public void setPartitionColumnsMap(Map<String, Set<String>> partitionColumnsMap) {
        this.partitionColumnsMap = partitionColumnsMap;
    }

    public List<Map<String, Object>> getResultSet() {
        return resultSet;
    }

    public void setResultSet(List<Map<String, Object>> resultSet) {
        this.resultSet = resultSet;
    }

    public List<InternalOutputRecord> getInternalResults() {
        return internalResults;
    }

    public void setInternalResults(List<InternalOutputRecord> internalResults) {
        this.internalResults = internalResults;
    }

    public boolean isReturnUnmatched() {
        return true;
    }

    public boolean isSeekingIdOnly() {
        return seekingIdOnly;
    }

    public void setSeekingIdOnly(boolean seekingIdOnly) {
        this.seekingIdOnly = seekingIdOnly;
    }

    public void setMatchEngine(MatchEngine matchEngine) {
        this.matchEngine = matchEngine;
    }

    public String getContextId() {
        return contextId;
    }

    public void setContextId(String contextId) {
        this.contextId = contextId;
    }

    public boolean isCDLMatch() {
        return isCDLMatch;
    }

    public void setCDLMatch(boolean CDLMatch) {
        isCDLMatch = CDLMatch;
    }

    public DynamoDataUnit getCustomAccountDataUnit() {
        return customAccountDataUnit;
    }

    public void setCustomAccountDataUnit(DynamoDataUnit customAccountDataUnit) {
        this.customAccountDataUnit = customAccountDataUnit;
    }

    @JsonIgnore
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

    public List<Future<Object>> getFuturesResult() {
        return futuresResult;
    }

    public void setFuturesResult(List<Future<Object>> futuresResult) {
        this.futuresResult = futuresResult;
    }

}
