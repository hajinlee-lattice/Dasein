package com.latticeengines.pls.end2end;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class ComparedRecord {

    private Object id;

    private double expectedScore;

    private double scoreApiScore;

    private double scoreDifference;

    private List<FieldConflict> transformFieldConflicts;

    private List<FieldConflict> matchedFieldConflicts;

    private int numMatchFieldConflicts;

    private int numTransformFieldConflicts;

    @JsonIgnore
    private Map<String, Object> expectedTransformedRecord;

    @JsonIgnore
    private Map<String, Object> scoreApiTransformedRecord;

    @JsonIgnore
    private Map<String, Object> expectedMatchedRecord;

    @JsonIgnore
    private Map<String, Object> scoreApiMatchedRecord;

    @JsonIgnore
    private List<String> scoreApiMatchExtraFields;

    @JsonIgnore
    private List<String> scoreApiMatchMissingFields;

    @JsonIgnore
    private List<String> scoreApiTransformExtraFields;

    @JsonIgnore
    private List<String> scoreApiTransformMissingFields;

    public Object getId() {
        return id;
    }

    public void setId(Object id) {
        this.id = id;
    }

    public Map<String, Object> getExpectedTransformedRecord() {
        return expectedTransformedRecord;
    }

    public void setExpectedTransformedRecord(Map<String, Object> expectedTransformedRecord) {
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

    public List<String> getScoreApiTransformExtraFields() {
        return scoreApiTransformExtraFields;
    }

    public void setScoreApiTransformExtraFields(List<String> scoreApiExtraFields) {
        this.scoreApiTransformExtraFields = scoreApiExtraFields;
    }

    public List<String> getScoreApiTransformMissingFields() {
        return scoreApiTransformMissingFields;
    }

    public void setScoreApiTransformMissingFields(List<String> scoreApiMissingFields) {
        this.scoreApiTransformMissingFields = scoreApiMissingFields;
    }

    public List<FieldConflict> getTransformFieldConflicts() {
        return transformFieldConflicts;
    }

    public void setTransformFieldConflicts(List<FieldConflict> fieldConflicts) {
        this.transformFieldConflicts = fieldConflicts;
    }

    public Map<String, Object> getExpectedMatchedRecord() {
        return expectedMatchedRecord;
    }

    public void setExpectedMatchedRecord(Map<String, Object> expectedMatchedRecord) {
        this.expectedMatchedRecord = expectedMatchedRecord;
    }

    public Map<String, Object> getScoreApiMatchedRecord() {
        return scoreApiMatchedRecord;
    }

    public void setScoreApiMatchedRecord(Map<String, Object> scoreApiMatchedRecord) {
        this.scoreApiMatchedRecord = scoreApiMatchedRecord;
    }

    public List<FieldConflict> getMatchedFieldConflicts() {
        return matchedFieldConflicts;
    }

    public void setMatchedFieldConflicts(List<FieldConflict> matchedFieldConflicts) {
        this.matchedFieldConflicts = matchedFieldConflicts;
    }

    public int getNumMatchFieldConflicts() {
        return numMatchFieldConflicts;
    }

    public void setNumMatchFieldConflicts(int numMatchFieldConflicts) {
        this.numMatchFieldConflicts = numMatchFieldConflicts;
    }

    public int getNumTransformFieldConflicts() {
        return numTransformFieldConflicts;
    }

    public void setNumTransformFieldConflicts(int numTransformFieldConflicts) {
        this.numTransformFieldConflicts = numTransformFieldConflicts;
    }

    public List<String> getScoreApiMatchExtraFields() {
        return scoreApiMatchExtraFields;
    }

    public void setScoreApiMatchExtraFields(List<String> scoreApiMatchExtraFields) {
        this.scoreApiMatchExtraFields = scoreApiMatchExtraFields;
    }

    public List<String> getScoreApiMatchMissingFields() {
        return scoreApiMatchMissingFields;
    }

    public void setScoreApiMatchMissingFields(List<String> scoreApiMatchMissingFields) {
        this.scoreApiMatchMissingFields = scoreApiMatchMissingFields;
    }

}
