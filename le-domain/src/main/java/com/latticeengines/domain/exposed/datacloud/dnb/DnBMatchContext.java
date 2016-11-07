package com.latticeengines.domain.exposed.datacloud.dnb;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricFieldGroup;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

public class DnBMatchContext implements Fact, Dimension {

    private NameLocation inputNameLocation;

    private String inputEmail;

    private String duns;

    private Integer confidenceCode;

    private DnBMatchOutput.MatchGrade matchGrade;

    private DnBReturnCode dnbCode;

    private Boolean hitWhiteCache = false;

    private Boolean hitBlackCache = false;

    @MetricFieldGroup
    public NameLocation getInputNameLocation() {
        return inputNameLocation;
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
    public DnBMatchOutput.MatchGrade getMatchGrade() {
        return matchGrade;
    }

    public void setMatchGrade(String matchGrade) {
        this.matchGrade = new DnBMatchOutput.MatchGrade(matchGrade);
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

    public static class MatchGrade implements Fact {

        private final String rawCode;

        public MatchGrade(String rawCode) {
            this.rawCode = rawCode;
        }

        @MetricField(name = "DnBMatchGrade")
        public String getRawCode() {
            return rawCode;
        }
    }

}
