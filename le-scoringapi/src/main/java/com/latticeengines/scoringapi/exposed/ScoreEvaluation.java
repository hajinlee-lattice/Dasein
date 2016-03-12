package com.latticeengines.scoringapi.exposed;

public class ScoreEvaluation {

    private double probability;
    private int percentile;

    public ScoreEvaluation(double probability, int percentile) {
        super();
        this.probability = probability;
        this.percentile = percentile;
    }

    public double getProbability() {
        return probability;
    }
    public void setProbability(double probability) {
        this.probability = probability;
    }
    public int getPercentile() {
        return percentile;
    }
    public void setPercentile(int percentile) {
        this.percentile = percentile;
    }

}
