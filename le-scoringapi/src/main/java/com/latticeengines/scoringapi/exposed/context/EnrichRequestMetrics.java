package com.latticeengines.scoringapi.exposed.context;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;

public class EnrichRequestMetrics implements Dimension, Fact {

    private Integer parseUuidDurationMS;

    private String tenantId;
    private Boolean hasWarning;
    private Boolean isEnrich;
    private String source;

    private Integer requestPreparationDurationMS;
    private Integer matchRecordDurationMS;
    private Integer requestDurationMS;

    @MetricTag(tag = "TenantId")
    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    @MetricTag(tag = "HasWarning")
    public String hasWarning() {
        return String.valueOf(hasWarning);
    }

    public void setHasWarning(boolean hasWarning) {
        this.hasWarning = hasWarning;
    }

    @MetricTag(tag = "IsEnrich")
    public String isEnrich() {
        return String.valueOf(isEnrich);
    }

    public void setIsEnrich(Boolean isEnrich) {
        this.isEnrich = isEnrich;
    }

    @MetricTag(tag = "Source")
    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    @MetricField(name = "RequestPreparationDurationMS", fieldType = MetricField.FieldType.INTEGER)
    public Integer getRequestPreparationDurationMS() {
        return requestPreparationDurationMS;
    }

    public void setRequestPreparationDurationMS(int requestPreparationDurationMS) {
        this.requestPreparationDurationMS = requestPreparationDurationMS;
    }

    @MetricField(name = "MatchRecordDurationMS", fieldType = MetricField.FieldType.INTEGER)
    public Integer getMatchRecordDurationMS() {
        return matchRecordDurationMS;
    }

    public void setMatchRecordDurationMS(int matchRecordDurationMS) {
        this.matchRecordDurationMS = matchRecordDurationMS;
    }

    @MetricField(name = "RequestDurationMS", fieldType = MetricField.FieldType.INTEGER)
    public Integer getRequestDurationMS() {
        return requestDurationMS;
    }

    public void setRequestDurationMS(int requestDurationMS) {
        this.requestDurationMS = requestDurationMS;
    }

    @MetricField(name = "ParseUuidDurationMS", fieldType = MetricField.FieldType.INTEGER)
    public Integer getParseUuidDurationMS() {
        return parseUuidDurationMS;
    }

    public void setParseUuidDurationMS(int parseUuidDurationMS) {
        this.parseUuidDurationMS = parseUuidDurationMS;
    }
}
