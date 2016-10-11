package com.latticeengines.datacloud.match.service.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricFieldGroup;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.common.exposed.metric.annotation.MetricTagGroup;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;
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
    private boolean returnUnmatched;
    private Long numRows;
    private MatchEngine matchEngine;

    private AccountLookupRequest accountLookupRequest;
    private List<LatticeAccount> matchedAccounts;
    private Map<String, String> latticeIdToLookupIdMap;

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

    public Map<String, List<Map<String, Object>>> getResultsByPartition() {
        return resultsByPartition;
    }

    public void setResultsByPartition(Map<String, List<Map<String, Object>>> resultsByPartition) {
        this.resultsByPartition = resultsByPartition;
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

    public AccountLookupRequest getAccountLookupRequest() {
        return accountLookupRequest;
    }

    public void setAccountLookupRequest(AccountLookupRequest accountLookupRequest) {
        this.accountLookupRequest = accountLookupRequest;
    }

    public List<LatticeAccount> getMatchedAccounts() {
        return matchedAccounts;
    }

    public void setMatchedAccounts(List<LatticeAccount> matchedAccounts) {
        this.matchedAccounts = matchedAccounts;
    }

    public Map<String, String> getLatticeIdToLookupIdMap() {
        return latticeIdToLookupIdMap;
    }

    public void setLatticeIdToLookupIdMap(Map<String, String> latticeIdToLookupIdMap) {
        this.latticeIdToLookupIdMap = latticeIdToLookupIdMap;
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
}
