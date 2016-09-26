package com.latticeengines.scoringapi.exposed.context;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;

public class ScoreRequestMetrics implements Dimension, Fact {

    private String rule;
    private String modelId;
    private Integer score;
    private Integer getTenantFromOAuthDurationMS;
    private Integer retrieveModelArtifactsDurationMS;
    private Integer parseRecordDurationMS;
    private Integer transformRecordDurationMS;
    private Integer scoreRecordDurationMS;

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

    @MetricField(name = "Rule", fieldType = MetricField.FieldType.STRING)
    public String getRule() {
        return rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }

    @MetricField(name = "ModelId", fieldType = MetricField.FieldType.STRING)
    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    @MetricField(name = "Score", fieldType = MetricField.FieldType.INTEGER)
    public Integer getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    @MetricField(name = "GetTenantFromOAuthDurationMS", fieldType = MetricField.FieldType.INTEGER)
    public Integer getGetTenantFromOAuthDurationMS() {
        return getTenantFromOAuthDurationMS;
    }

    public void setGetTenantFromOAuthDurationMS(int getTenantFromOAuthDurationMS) {
        this.getTenantFromOAuthDurationMS = getTenantFromOAuthDurationMS;
    }

    @MetricField(name = "RetrieveModelArtifactsDurationMS", fieldType = MetricField.FieldType.INTEGER)
    public Integer getRetrieveModelArtifactsDurationMS() {
        return retrieveModelArtifactsDurationMS;
    }

    public void setRetrieveModelArtifactsDurationMS(int retrieveModelArtifactsDurationMS) {
        this.retrieveModelArtifactsDurationMS = retrieveModelArtifactsDurationMS;
    }

    @MetricField(name = "ParseRecordDurationMS", fieldType = MetricField.FieldType.INTEGER)
    public Integer getParseRecordDurationMS() {
        return parseRecordDurationMS;
    }

    public void setParseRecordDurationMS(int parseRecordDurationMS) {
        this.parseRecordDurationMS = parseRecordDurationMS;
    }

    @MetricField(name = "TransformRecordDurationMS", fieldType = MetricField.FieldType.INTEGER)
    public Integer getTransformRecordDurationMS() {
        return transformRecordDurationMS;
    }

    public void setTransformRecordDurationMS(int transformRecordDurationMS) {
        this.transformRecordDurationMS = transformRecordDurationMS;
    }

    @MetricField(name = "ScoreRecordDurationMS", fieldType = MetricField.FieldType.INTEGER)
    public Integer getScoreRecordDurationMS() {
        return scoreRecordDurationMS;
    }

    public void setScoreRecordDurationMS(int scoreRecordDurationMS) {
        this.scoreRecordDurationMS = scoreRecordDurationMS;
    }

}
