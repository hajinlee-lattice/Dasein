package com.latticeengines.domain.exposed.datacloud.dnb;

import java.util.Date;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricFieldGroup;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

public class DnBMatchContext implements Fact, Dimension {

    private NameLocation inputNameLocation;

    private String inputEmail;

    private String duns;

    private String finalDuns;

    private String finalDuDuns;

    private String finalGuDuns;

    private String origDuns;

    private NameLocation matchedNameLocation;

    private Integer confidenceCode;

    private DnBMatchGrade matchGrade;

    private DnBReturnCode dnbCode;

    private Boolean hitWhiteCache = false;

    private Boolean hitBlackCache = false;

    private String cacheId;

    private String lookupRequestId;

    private String serviceBatchId;

    private DnBMatchStrategy matchStrategy;

    private Long duration;

    private boolean logDnBBulkResult;

    private Boolean patched;

    private long timestamp;

    private Boolean outOfBusiness;

    private Boolean dunsInAM;

    private String dataCloudVersion;

    private boolean calledRemoteDnB;

    private Date requestTime;

    private Date responseTime;

    private boolean passAcceptanceCriteria;

    private String rootOperationUid;

    public DnBMatchContext() {
        inputNameLocation = new NameLocation();
        matchedNameLocation = new NameLocation();
    }

    public void copyMatchInput(DnBMatchContext context) {
        inputNameLocation = context.getInputNameLocation();
        inputEmail = context.getInputEmail();
        hitWhiteCache = context.getHitWhiteCache();
        hitBlackCache = context.getHitBlackCache();
        cacheId = context.getCacheId();
        lookupRequestId = context.getLookupRequestId();
        matchStrategy = context.getMatchStrategy();
        logDnBBulkResult = context.getLogDnBBulkResult();
        patched = context.getPatched();
        timestamp = context.getTimestamp(); // Used to check timeout for each
                                            // record in DnB bulk match
        dataCloudVersion = context.getDataCloudVersion();
    }

    // Used to copy bulk match result. Should not copy dataCloudVersion &
    // dunsInAM
    public void copyMatchResult(DnBMatchContext result) {
        duns = result.getDuns();
        dnbCode = result.getDnbCode();
        confidenceCode = result.getConfidenceCode();
        matchGrade = result.getMatchGrade();
        lookupRequestId = result.getLookupRequestId();
        cacheId = result.cacheId;
        matchedNameLocation = result.getMatchedNameLocation();
        outOfBusiness = result.isOutOfBusiness();
    }

    public void copyResultFromCache(DnBCache cache) {
        if (cache.isWhiteCache()) {
            duns = cache.getDuns();
            origDuns = cache.getDuns();
            dnbCode = DnBReturnCode.OK;
            confidenceCode = cache.getConfidenceCode();
            matchGrade = cache.getMatchGrade();
            cacheId = cache.getId();
            matchedNameLocation = cache.getMatchedNameLocation();
            outOfBusiness = cache.isOutOfBusiness();
            dunsInAM = cache.isDunsInAM();
            hitWhiteCache = true;
            patched = cache.getPatched();
        } else {
            duns = null;
            origDuns = cache.getDuns();
            dnbCode = DnBReturnCode.UNMATCH;
            cacheId = cache.getId();
            confidenceCode = null;
            matchGrade = null;
            hitBlackCache = true;
        }
    }

    /**
     * Reset all the result copied from
     * {@link DnBMatchContext#copyResultFromCache(DnBCache)} for white cache.
     * Noop if the result is copied from black cache
     */
    public void clearWhiteCacheResult() {
        if (!hitWhiteCache) {
            return;
        }

        duns = null;
        origDuns = null;
        dnbCode = null;
        confidenceCode = null;
        matchGrade = null;
        cacheId = null;
        matchedNameLocation = null;
        outOfBusiness = null;
        dunsInAM = null;
        hitWhiteCache = false;
        patched = null;
    }

    @MetricFieldGroup
    public NameLocation getInputNameLocation() {
        return inputNameLocation;
    }

    public void setInputNameLocation(MatchKeyTuple matchKeyTuple) {
        inputNameLocation.setName(matchKeyTuple.getName());
        inputNameLocation.setCountry(matchKeyTuple.getCountry());
        inputNameLocation.setCountryCode(matchKeyTuple.getCountryCode());
        inputNameLocation.setState(matchKeyTuple.getState());
        inputNameLocation.setCity(matchKeyTuple.getCity());
        inputNameLocation.setPhoneNumber(matchKeyTuple.getPhoneNumber());
        inputNameLocation.setZipcode(matchKeyTuple.getZipcode());
    }

    public void setInputNameLocation(NameLocation inputNameLocation) {
        this.inputNameLocation = inputNameLocation;
    }

    @MetricField(name = "Email")
    public String getInputEmail() {
        return inputEmail;
    }

    public void setInputEmail(String inputEmail) {
        this.inputEmail = inputEmail;
    }

    @MetricField(name = "DUNS")
    public String getDuns() {
        return duns;
    }

    public void setDuns(String duns) {
        if (StringUtils.isNotEmpty(duns)) {
            this.duns = duns;
        } else {
            this.duns = null;
        }
    }

    public String getFinalDuns() {
        return finalDuns;
    }

    public void setFinalDuns(String finalDuns) {
        this.finalDuns = finalDuns;
    }

    public String getFinalDuDuns() {
        return finalDuDuns;
    }

    public void setFinalDuDuns(String finalDuDuns) {
        this.finalDuDuns = finalDuDuns;
    }

    public String getFinalGuDuns() {
        return finalGuDuns;
    }

    public void setFinalGuDuns(String finalGuDuns) {
        this.finalGuDuns = finalGuDuns;
    }

    public String getOrigDuns() {
        return origDuns;
    }

    public void setOrigDuns(String origDuns) {
        this.origDuns = origDuns;
    }

    public NameLocation getMatchedNameLocation() {
        return matchedNameLocation;
    }

    public void setMatchedNameLocation(NameLocation matchedNameLocation) {
        this.matchedNameLocation = matchedNameLocation;
    }

    @MetricField(name = "DnBConfidenceCode", fieldType = MetricField.FieldType.INTEGER)
    public Integer getConfidenceCode() {
        return confidenceCode;
    }

    public void setConfidenceCode(Integer confidenceCode) {
        this.confidenceCode = confidenceCode;
    }

    @MetricFieldGroup
    public DnBMatchGrade getMatchGrade() {
        return matchGrade;
    }

    public void setMatchGrade(DnBMatchGrade matchGrade) {
        this.matchGrade = matchGrade;
    }

    public void setMatchGrade(String matchGrade) {
        if (StringUtils.isNotEmpty(matchGrade)) {
            this.matchGrade = new DnBMatchGrade(matchGrade);
        } else {
            this.matchGrade = null;
        }

    }

    @MetricField(name = "DnbCode")
    public String getDnbCodeAsString() {
        if (dnbCode != null) {
            return dnbCode.getMessage();
        } else {
            return null;
        }
    }

    public DnBReturnCode getDnbCode() {
        return dnbCode;
    }

    public void setDnbCode(DnBReturnCode dnbCode) {
        this.dnbCode = dnbCode;
    }

    public String getCacheId() {
        return cacheId;
    }

    public void setCacheId(String cacheId) {
        this.cacheId = cacheId;
    }

    @MetricField(name = "HitWhiteCache", fieldType = MetricField.FieldType.BOOLEAN)
    public Boolean getHitWhiteCache() {
        return hitWhiteCache;
    }

    public void setHitWhiteCache(Boolean hitWhiteCache) {
        this.hitWhiteCache = hitWhiteCache;
    }

    @MetricField(name = "HitBlackCache", fieldType = MetricField.FieldType.BOOLEAN)
    public Boolean getHitBlackCache() {
        return hitBlackCache;
    }

    public void setHitBlackCache(Boolean hitBlackCache) {
        this.hitBlackCache = hitBlackCache;
    }

    @MetricField(name = "LookupRequestId", fieldType = MetricField.FieldType.STRING)
    public String getLookupRequestId() {
        return lookupRequestId;
    }

    public void setLookupRequestId(String lookupRequestId) {
        this.lookupRequestId = lookupRequestId;
    }

    public DnBMatchStrategy getMatchStrategy() {
        return matchStrategy;
    }

    public void setMatchStrategy(DnBMatchStrategy matchStrategy) {
        this.matchStrategy = matchStrategy;
    }

    @MetricTag(tag = "MatchStrategy")
    public String getMatchStrategyName() {
        return getMatchStrategy().name();
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    @MetricField(name = "Duration", fieldType = MetricField.FieldType.DOUBLE)
    public Double getDurationAsDouble() {
        if (duration != null) {
            return duration.doubleValue();
        } else {
            return null;
        }
    }

    public String getServiceBatchId() {
        return serviceBatchId;
    }

    public void setServiceBatchId(String serviceBatchId) {
        this.serviceBatchId = serviceBatchId;
    }

    public boolean getLogDnBBulkResult() {
        return logDnBBulkResult;
    }

    public void setLogDnBBulkResult(boolean logDnBBulkResult) {
        this.logDnBBulkResult = logDnBBulkResult;
    }

    @MetricField(name = "Patched", fieldType = MetricField.FieldType.BOOLEAN)
    public Boolean getPatched() {
        return patched;
    }

    public void setPatched(Boolean patched) {
        this.patched = patched;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Boolean isOutOfBusiness() {
        return outOfBusiness;
    }

    public void setOutOfBusiness(Boolean outOfBusiness) {
        this.outOfBusiness = outOfBusiness;
    }

    public String isOutOfBusinessString() {
        return outOfBusiness == null ? null : String.valueOf(outOfBusiness);
    }

    public Boolean isDunsInAM() {
        return dunsInAM;
    }

    public void setDunsInAM(Boolean dunsInAM) {
        this.dunsInAM = dunsInAM;
    }

    public String isDunsInAMString() {
        return dunsInAM == null ? null : String.valueOf(dunsInAM);
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

    public boolean isCalledRemoteDnB() {
        return calledRemoteDnB;
    }

    public void setCalledRemoteDnB(boolean calledRemoteDnB) {
        this.calledRemoteDnB = calledRemoteDnB;
    }

    public Date getRequestTime() {
        return requestTime;
    }

    public void setRequestTime(Date requestTime) {
        this.requestTime = requestTime;
    }

    public Date getResponseTime() {
        return responseTime;
    }

    public void setResponseTime(Date responseTime) {
        this.responseTime = responseTime;
    }

    public boolean isPassAcceptanceCriteria() {
        return passAcceptanceCriteria;
    }

    public void setPassAcceptanceCriteria(boolean passAcceptanceCriteria) {
        this.passAcceptanceCriteria = passAcceptanceCriteria;
    }

    public String getRootOperationUid() {
        return rootOperationUid;
    }

    public void setRootOperationUid(String rootOperationUid) {
        this.rootOperationUid = rootOperationUid;
    }

    public enum DnBMatchStrategy {
        EMAIL, ENTITY, BATCH
    }

}
