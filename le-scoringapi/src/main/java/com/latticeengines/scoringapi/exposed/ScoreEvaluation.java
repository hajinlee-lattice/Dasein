package com.latticeengines.scoringapi.exposed;

import com.latticeengines.domain.exposed.pls.BucketName;

public class ScoreEvaluation {

    private double probabilityOrValue;
    private int percentile;
    private ScoreType scoreType = null;
    private String classification;
    private BucketName bucketName;

    public ScoreEvaluation(double probabilityOrValue, int percentile) {
        this(probabilityOrValue, percentile, null);
    }

    public ScoreEvaluation(double probabilityOrValue, int percentile, BucketName bucketName) {
        super();
        this.probabilityOrValue = probabilityOrValue;
        this.percentile = percentile;
        this.bucketName = bucketName;
    }

    public double getProbabilityOrValue() {
        return probabilityOrValue;
    }

    public void setProbabilityOrValue(double probabilityOrValue) {
        this.probabilityOrValue = probabilityOrValue;
    }

    public int getPercentile() {
        return percentile;
    }

    public void setPercentile(int percentile) {
        this.percentile = percentile;
    }

    public ScoreType getScoreType() {
        return scoreType;
    }

    public void setScoreType(ScoreType scoreType) {
        this.scoreType = scoreType;
    }

    public String getClassification() {
        return classification;
    }

    public void setClassification(String classification) {
        this.classification = classification;
    }

    public BucketName getBucketName() {
        return bucketName;
    }

    public void setBucketName(BucketName bucketName) {
        this.bucketName = bucketName;
    }

}
