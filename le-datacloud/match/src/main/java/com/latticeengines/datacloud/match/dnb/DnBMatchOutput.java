package com.latticeengines.datacloud.match.dnb;

import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricFieldGroup;

public class DnBMatchOutput implements Fact {

    private String duns;

    private Integer confidenceCode;

    private DnBMatchGrade matchGrade;

    private DnBReturnCode dnbCode;

    private Boolean hitCache = false;

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

    @MetricField(name = "HitDnBCache", fieldType = MetricField.FieldType.BOOLEAN)
    public Boolean getHitCache() {
        return hitCache;
    }

    public void setHitCache(Boolean hitCache) {
        this.hitCache = hitCache;
    }
    
}
