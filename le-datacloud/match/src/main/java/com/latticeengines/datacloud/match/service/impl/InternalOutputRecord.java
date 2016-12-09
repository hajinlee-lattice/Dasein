package com.latticeengines.datacloud.match.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;

public class InternalOutputRecord extends OutputRecord {

    private String parsedDomain;
    private Boolean isPublicDomain = false;
    private boolean matchEvenIsPublicDomain;
    private String parsedDuns;
    private NameLocation parsedNameLocation;
    private String parsedEmail;
    private Map<String, Map<String, Object>> resultsInPartition = new HashMap<>();
    private Map<String, Object> queryResult = new HashMap<>();
    private List<Boolean> columnMatched;
    private Boolean failed = false;
    private String travelerId;
    private String latticeAccountId;
    private LatticeAccount latticeAccount;
    private String originalContextId;

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

    public boolean isMatchEvenIsPublicDomain() {
        return matchEvenIsPublicDomain;
    }

    public void setMatchEvenIsPublicDomain(boolean matchEvenIsPublicDomain) {
        this.matchEvenIsPublicDomain = matchEvenIsPublicDomain;
    }

    public String getParsedDuns() {
        return parsedDuns;
    }

    public void setParsedDuns(String parsedDuns) {
        this.parsedDuns = parsedDuns;
    }

    public NameLocation getParsedNameLocation() {
        return parsedNameLocation;
    }

    public void setParsedNameLocation(NameLocation parsedNameLocation) {
        this.parsedNameLocation = parsedNameLocation;
    }

    public String getParsedEmail() {
        return parsedEmail;
    }

    public void setParsedEmail(String parsedEmail) {
        this.parsedEmail = parsedEmail;
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

    public String getTravelerId() {
        return travelerId;
    }

    public void setTravelerId(String travelerId) {
        this.travelerId = travelerId;
    }

    public String getLatticeAccountId() {
        return latticeAccountId;
    }

    public void setLatticeAccountId(String latticeAccountId) {
        this.latticeAccountId = latticeAccountId;
    }

    public LatticeAccount getLatticeAccount() {
        return latticeAccount;
    }

    public void setLatticeAccount(LatticeAccount latticeAccount) {
        this.latticeAccount = latticeAccount;
    }

    public String getOriginalContextId() {
        return originalContextId;
    }

    public void setOriginalContextId(String originalContextId) {
        this.originalContextId = originalContextId;
    }
}
