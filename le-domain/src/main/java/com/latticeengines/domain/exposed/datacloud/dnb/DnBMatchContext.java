package com.latticeengines.domain.exposed.datacloud.dnb;

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

    public DnBMatchContext() {
        inputNameLocation = new NameLocation();
        matchedNameLocation = new NameLocation();
    }

    public void copyMatchResult(DnBMatchContext result) {
        duns = result.getDuns();
        dnbCode = result.getDnbCode();
        confidenceCode = result.getConfidenceCode();
        matchGrade = result.getMatchGrade();
        lookupRequestId = result.getLookupRequestId();
        cacheId = result.cacheId;
        matchedNameLocation = result.getMatchedNameLocation();
    }

    public void copyResultFromCache(DnBCache cache) {
        if (cache.isWhiteCache()) {
            duns = cache.getDuns();
            dnbCode = DnBReturnCode.OK;
            confidenceCode = cache.getConfidenceCode();
            matchGrade = cache.getMatchGrade();
            cacheId = cache.getId();
            matchedNameLocation = cache.getMatchedNameLocation();
            hitWhiteCache = true;
            patched = cache.getPatched();
        } else {
            duns = null;
            dnbCode = DnBReturnCode.UNMATCH;
            cacheId = cache.getId();
            confidenceCode = null;
            matchGrade = null;
            hitBlackCache = true;
        }
    }

    @MetricFieldGroup
    public NameLocation getInputNameLocation() {
        return inputNameLocation;
    }

    public void setInputNameLocation(NameLocation inputNameLocation) {
        this.inputNameLocation = inputNameLocation;
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
        this.duns = duns;
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

    public void setMatchGrade(String matchGrade) {
        this.matchGrade = new DnBMatchGrade(matchGrade);
    }

    public void setMatchGrade(DnBMatchGrade matchGrade) {
        this.matchGrade = matchGrade;
    }

    @MetricField(name = "DnbCode")
    public String getDnbCodeAsString() {
        if (dnbCode != null) {
            return dnbCode.getMessage();
        } else  {
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

    public enum DnBMatchStrategy {
        EMAIL, ENTITY, BATCH
    }

}
