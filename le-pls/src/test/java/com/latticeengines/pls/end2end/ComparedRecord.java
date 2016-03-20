package com.latticeengines.pls.end2end;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class ComparedRecord {

    private Object id;

    @JsonIgnore
    private Map<String, String> expectedTransformedRecord;

    @JsonIgnore
    private Map<String, Object> scoreApiTransformedRecord;

    private double expectedScore;

    private double scoreApiScore;

    private double scoreDifference;

    private List<String> scoreApiExtraFields;

    private List<String> scoreApiMissingFields;

    private List<FieldConflict> fieldConflicts;

    public Object getId() {
        return id;
    }

    public void setId(Object id) {
        this.id = id;
    }

    public Map<String, String> getExpectedTransformedRecord() {
        return expectedTransformedRecord;
    }

    public void setExpectedTransformedRecord(Map<String, String> expectedTransformedRecord) {
        this.expectedTransformedRecord = expectedTransformedRecord;
    }

    public Map<String, Object> getScoreApiTransformedRecord() {
        return scoreApiTransformedRecord;
    }

    public void setScoreApiTransformedRecord(Map<String, Object> scoreApiTransformedRecord) {
        this.scoreApiTransformedRecord = scoreApiTransformedRecord;
    }

    public double getExpectedScore() {
        return expectedScore;
    }

    public void setExpectedScore(double expectedScore) {
        this.expectedScore = expectedScore;
    }

    public double getScoreApiScore() {
        return scoreApiScore;
    }

    public void setScoreApiScore(double scoreApiScore) {
        this.scoreApiScore = scoreApiScore;
    }

    public double getScoreDifference() {
        return scoreDifference;
    }

    public void setScoreDifference(double scoreDifference) {
        this.scoreDifference = scoreDifference;
    }

    public List<String> getScoreApiExtraFields() {
        return scoreApiExtraFields;
    }

    public void setScoreApiExtraFields(List<String> scoreApiExtraFields) {
        this.scoreApiExtraFields = scoreApiExtraFields;
    }

    public List<String> getScoreApiMissingFields() {
        return scoreApiMissingFields;
    }

    public void setScoreApiMissingFields(List<String> scoreApiMissingFields) {
        this.scoreApiMissingFields = scoreApiMissingFields;
    }

    public List<FieldConflict> getFieldConflicts() {
        return fieldConflicts;
    }

    public void setFieldConflicts(List<FieldConflict> fieldConflicts) {
        this.fieldConflicts = fieldConflicts;
    }

}
