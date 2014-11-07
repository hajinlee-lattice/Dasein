package com.latticeengines.skald;

public class ScoreResult
{
    public ScoreResult(double probability, double lift, int percentile, int score, String potential)
    {
       this.probability = probability;
       this.lift = lift;
       this.percentile = percentile;
       this.score = score;
       this.potential = potential;
    }
    
    public final double probability;
    public final double lift;
    public final int percentile;
    public final int score;
    public final String potential;
}
