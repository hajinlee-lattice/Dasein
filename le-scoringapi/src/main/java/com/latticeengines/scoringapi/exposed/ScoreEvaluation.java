package com.latticeengines.scoringapi.exposed;

public class ScoreEvaluation {

    private double probabilityOrValue;
    private int percentile;

    public ScoreEvaluation(double probabilityOrValue, int percentile) {
        super();
        this.probabilityOrValue = probabilityOrValue;
        this.percentile = percentile;
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

}
