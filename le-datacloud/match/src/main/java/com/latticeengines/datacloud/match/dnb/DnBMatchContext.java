package com.latticeengines.datacloud.match.dnb;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricFieldGroup;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

public class DnBMatchContext implements Fact, Dimension {

    private NameLocation inputNameLocation;

    private String inputEmail;

    private String duns;

    private Integer confidenceCode;

    private DnBMatchGrade matchGrade;

    private DnBReturnCode dnbCode;

    private Boolean hitWhiteCache = false;

    private Boolean hitBlackCache = false;

    private String lookupRequestId;

    private DnBMatchStrategy matchStrategy;

    public DnBMatchContext() {
        inputNameLocation = new NameLocation();
    }

    public void copyMatchResult(DnBMatchContext result) {
        duns = result.getDuns();
        dnbCode = result.getDnbCode();
        confidenceCode = result.getConfidenceCode();
        matchGrade = result.getMatchGrade();
        lookupRequestId = result.getLookupRequestId();
    }

    public void copyResultFromWhiteCache(DnBWhiteCache whiteCache) {
        duns = whiteCache.getDuns();
        dnbCode = DnBReturnCode.OK;
        confidenceCode = whiteCache.getConfidenceCode();
        matchGrade = whiteCache.getMatchGrade();
        hitWhiteCache = true;
    }

    public void copyResultFromBlackCache(DnBBlackCache blackCache) {
        duns = null;
        dnbCode = DnBReturnCode.UNMATCH;
        confidenceCode = null;
        matchGrade = null;
        hitBlackCache = true;
    }

    @MetricFieldGroup
    public NameLocation getInputNameLocation() {
        return inputNameLocation;
    }

    public void setInputNameLocation(NameLocation inputNameLocation) {
        this.inputNameLocation = inputNameLocation;
    }

    public void setInputNameLocation(MatchKeyTuple matchKeyTuple) {
        inputNameLocation = new NameLocation();
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

    public String getDuns() {
        return duns;
    }

    public void setDuns(String duns) {
        this.duns = duns;
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
        return dnbCode.getMessage();
    }

    public DnBReturnCode getDnbCode() {
        return dnbCode;
    }

    public void setDnbCode(DnBReturnCode dnbCode) {
        this.dnbCode = dnbCode;
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

    public enum DnBMatchStrategy {
        EMAIL, ENTITY
    }

}
