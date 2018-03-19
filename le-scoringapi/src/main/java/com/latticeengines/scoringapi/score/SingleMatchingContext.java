package com.latticeengines.scoringapi.score;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;
import com.latticeengines.scoringapi.exposed.model.ModelJsonTypeHandler;

public class SingleMatchingContext {
    private Map<String, Object> readyToTransformRecord = null;
    private Map<String, Object> transformedRecord = new HashMap<>();
    private Map<String, Object> enrichmentAttributes = new HashMap<>();
    private List<String> matchLogs = new ArrayList<>();
    private List<String> matchErrorLogs = new ArrayList<>();
    private ModelSummary modelSummary = null;
    private Map<String, FieldSchema> fieldSchemas = null;
    private ScoringArtifacts scoringArtifacts = null;
    private ModelJsonTypeHandler modelJsonTypeHandler = null;

    public static SingleMatchingContext instance() {
        return new SingleMatchingContext();
    }

    public Map<String, Object> getReadyToTransformRecord() {
        return readyToTransformRecord;
    }

    public SingleMatchingContext setTransformedRecord(Map<String, Object> transformedRecord) {
        this.transformedRecord = transformedRecord;
        return this;
    }

    public Map<String, Object> getTransformedRecord() {
        return transformedRecord;
    }

    public SingleMatchingContext setReadyToTransformRecord(Map<String, Object> readyToTransformRecord) {
        this.readyToTransformRecord = readyToTransformRecord;
        return this;
    }

    public Map<String, Object> getEnrichmentAttributes() {
        return enrichmentAttributes;
    }

    public SingleMatchingContext setEnrichmentAttributes(Map<String, Object> enrichmentAttributes) {
        this.enrichmentAttributes = enrichmentAttributes;
        return this;
    }

    public List<String> getMatchLogs() {
        return matchLogs;
    }

    public SingleMatchingContext setMatchLogs(List<String> matchLogs) {
        this.matchLogs = matchLogs;
        return this;
    }

    public List<String> getMatchErrorLogs() {
        return matchErrorLogs;
    }

    public SingleMatchingContext setMatchErrorLogs(List<String> matchErrorLogs) {
        this.matchErrorLogs = matchErrorLogs;
        return this;
    }

    public ModelSummary getModelSummary() {
        return modelSummary;
    }

    public SingleMatchingContext setModelSummary(ModelSummary modelSummary) {
        this.modelSummary = modelSummary;
        return this;
    }

    public Map<String, FieldSchema> getFieldSchemas() {
        return fieldSchemas;
    }

    public SingleMatchingContext setFieldSchemas(Map<String, FieldSchema> fieldSchemas) {
        this.fieldSchemas = fieldSchemas;
        return this;
    }

    public ScoringArtifacts getScoringArtifacts() {
        return scoringArtifacts;
    }

    public SingleMatchingContext setScoringArtifacts(ScoringArtifacts scoringArtifacts) {
        this.scoringArtifacts = scoringArtifacts;
        return this;
    }

    public ModelJsonTypeHandler getModelJsonTypeHandler() {
        return modelJsonTypeHandler;
    }

    public SingleMatchingContext setModelJsonTypeHandler(ModelJsonTypeHandler modelJsonTypeHandler) {
        this.modelJsonTypeHandler = modelJsonTypeHandler;
        return this;
    }

}
