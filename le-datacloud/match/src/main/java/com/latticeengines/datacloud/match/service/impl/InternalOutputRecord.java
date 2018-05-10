package com.latticeengines.datacloud.match.service.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;
import com.latticeengines.domain.exposed.datacloud.match.MatchHistory;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;

public class InternalOutputRecord extends OutputRecord {

    private String parsedDomain;
    private Boolean isPublicDomain = false;
    private boolean matchEvenIsPublicDomain;
    private String parsedDuns;
    private NameLocation parsedNameLocation;
    private String parsedEmail;
    private String origDomain;
    private NameLocation origNameLocation;
    private String origDuns;
    private String origEmail;

    private Map<String, Map<String, Object>> resultsInPartition = new HashMap<>();
    private Map<String, Object> queryResult = new HashMap<>();
    private List<Boolean> columnMatched;
    private Boolean failed = false;
    private String travelerId;
    private String latticeAccountId;

    private String lookupIdKey;
    private String lookupIdValue;

    private LatticeAccount latticeAccount;
    private Map<String, Object> customAccount;
    private String originalContextId;

    private List<String> debugValues;
    private DnBReturnCode dnbCode;
    private MatchHistory fabricMatchHistory = new MatchHistory();
    private Date requestTimeStamp = new Date();

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

    public String getOrigDomain() {
        return origDomain;
    }

    public void setOrigDomain(String origDomain) {
        this.origDomain = origDomain;
    }

    public NameLocation getOrigNameLocation() {
        return origNameLocation;
    }

    public void setOrigNameLocation(NameLocation origNameLocation) {
        this.origNameLocation = origNameLocation;
    }

    public String getOrigDuns() {
        return origDuns;
    }

    public void setOrigDuns(String origDuns) {
        this.origDuns = origDuns;
    }

    public String getOrigEmail() {
        return origEmail;
    }

    public void setOrigEmail(String origEmail) {
        this.origEmail = origEmail;
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

    public String getLookupIdKey() {
        return lookupIdKey;
    }

    public void setLookupIdKey(String lookupIdKey) {
        this.lookupIdKey = lookupIdKey;
    }

    public String getLookupIdValue() {
        return lookupIdValue;
    }

    public void setLookupIdValue(String lookupIdValue) {
        this.lookupIdValue = lookupIdValue;
    }

    public LatticeAccount getLatticeAccount() {
        return latticeAccount;
    }

    public void setLatticeAccount(LatticeAccount latticeAccount) {
        this.latticeAccount = latticeAccount;
    }

    public Map<String, Object> getCustomAccount() {
        return customAccount;
    }

    public void setCustomAccount(Map<String, Object> customAccount) {
        this.customAccount = customAccount;
    }

    public String getOriginalContextId() {
        return originalContextId;
    }

    public void setOriginalContextId(String originalContextId) {
        this.originalContextId = originalContextId;
    }

    public List<String> getDebugValues() {
        return debugValues;
    }

    public void setDebugValues(List<String> debugValues) {
        this.debugValues = debugValues;
    }

    public void setDnbCode(DnBReturnCode dnbCode) {
        this.dnbCode = dnbCode;
    }

    public DnBReturnCode getDnbCode() {
        return dnbCode;
    }

    public MatchHistory getFabricMatchHistory() {
        return fabricMatchHistory;
    }

    public void setFabricMatchHistory(MatchHistory fabricMatchHistory) {
        this.fabricMatchHistory = fabricMatchHistory;
    }

    public Date getRequestTimeStamp() {
        return requestTimeStamp;
    }

    public void setRequestTimeStamp(Date requestTimeStamp) {
        this.requestTimeStamp = requestTimeStamp;
    }

}
