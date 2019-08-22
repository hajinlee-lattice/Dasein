package com.latticeengines.datacloud.match.service.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchHistory;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;
import com.latticeengines.domain.exposed.datacloud.match.MatchHistory;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.security.Tenant;

public class InternalOutputRecord extends OutputRecord {

    private String parsedDomain;
    private Boolean isPublicDomain = false;
    private boolean matchEvenIsPublicDomain;
    private String parsedDuns;
    private NameLocation parsedNameLocation;
    private String parsedEmail;
    private Tenant parsedTenant;
    private Map<String, String> parsedSystemIds;

    private String origDomain;
    private NameLocation origNameLocation;
    private String origDuns;
    private String origEmail;
    private Tenant origTenant;
    private Map<String, String> origSystemIds;

    private Map<String, Map<String, Object>> resultsInPartition = new HashMap<>();
    private Map<String, Object> queryResult = new HashMap<>();
    private List<Boolean> columnMatched;
    private Boolean failed = false;
    private String travelerId;
    private String latticeAccountId;
    private String entityId;

    private String lookupIdKey;
    private String lookupIdValue;

    private LatticeAccount latticeAccount;
    private Map<String, Object> customAccount;
    private String originalContextId;

    private List<String> debugValues;
    private DnBReturnCode dnbCode;
    private MatchHistory fabricMatchHistory = new MatchHistory();
    private Date requestTimeStamp = new Date();
    private String dataCloudVersion;

    // field names that should be cleared out in the output
    private Set<String> fieldsToClear;

    // For Entity Match, the InternalOutputRecord must contain the map between the MatchKeys and the position of the
    // corresponding fields in the input record, for each Entity.  This is then passed on to the MatchTraveler and
    // is used by the Match Planner actor.
    // Entity -> (MatchKey -> list of field indexes in the input data record (above) which correspond to the match key)
    private Map<String, Map<MatchKey, List<Integer>>> entityKeyPositionMaps;

    // Match result: entity -> entityId
    private Map<String, String> entityIds;

    // Store the EntityMatchHistory so it can be passed between the MatchTraveler and the MatchHistory.
    private EntityMatchHistory entityMatchHistory;

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

    public Tenant getParsedTenant() {
        return parsedTenant;
    }

    public void setParsedTenant(Tenant parsedTenant) {
        this.parsedTenant = parsedTenant;
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

    public Tenant getOrigTenant() {
        return origTenant;
    }

    public void setOrigTenant(Tenant origTenant) {
        this.origTenant = origTenant;
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

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
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

    @Override
    public List<String> getDebugValues() {
        return debugValues;
    }

    @Override
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

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

    public Set<String> getFieldsToClear() {
        return fieldsToClear;
    }

    public void setFieldsToClear(Set<String> fieldsToClear) {
        this.fieldsToClear = fieldsToClear;
    }

    public Map<String, Map<MatchKey, List<Integer>>> getEntityKeyPositionMaps() {
        return entityKeyPositionMaps;
    }

    public void setEntityKeyPositionMap(Map<String, Map<MatchKey, List<Integer>>> entityKeyPositionMaps) {
        this.entityKeyPositionMaps = entityKeyPositionMaps;
    }

    public Map<String, String> getParsedSystemIds() {
        return parsedSystemIds;
    }

    public void setParsedSystemIds(Map<String, String> parsedSystemIds) {
        this.parsedSystemIds = parsedSystemIds;
    }

    public Map<String, String> getOrigSystemIds() {
        return origSystemIds;
    }

    public void setOrigSystemIds(Map<String, String> origSystemIds) {
        this.origSystemIds = origSystemIds;
    }

    public Map<String, String> getEntityIds() {
        return entityIds;
    }

    public void setEntityIds(Map<String, String> entityIds) {
        this.entityIds = entityIds;
    }

    public EntityMatchHistory getEntityMatchHistory() {
        return entityMatchHistory;
    }

    public void setEntityMatchHistory(EntityMatchHistory entityMatchHistory) {
        this.entityMatchHistory = entityMatchHistory;
    }
}
