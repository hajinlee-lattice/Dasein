package com.latticeengines.scoring.util;

public class NormalizationBucket {

    private double start;
    private double end;
    private double cumulativePercentage;

    public NormalizationBucket(double start, double end, double cumulativePercentage) {
        super();
        this.start = start;
        this.end = end;
        this.cumulativePercentage = cumulativePercentage;
    }

    public double getStart() {
        return start;
    }

    public void setStart(double start) {
        this.start = start;
    }

    public double getEnd() {
        return end;
    }

    public void setEnd(double end) {
        this.end = end;
    }

    public double getCumulativePercentage() {
        return cumulativePercentage;
    }

    public void setCumulativePercentage(double cumulativePercentage) {
        this.cumulativePercentage = cumulativePercentage;
    }

}
