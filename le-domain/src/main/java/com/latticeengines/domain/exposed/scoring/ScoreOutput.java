package com.latticeengines.domain.exposed.scoring;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;

@Entity
@IdClass(ScoreOutput.ScoreOutputPK.class)
public class ScoreOutput{

    @Id
    @Column(name = "LeadID", nullable = true)
    private String LeadId;

    @Id
    @Column(name = "Play_Display_Name", nullable = true)
    private String Play_Display_Name;

    @Column(name = "Score", nullable = true)
    private int Score;

    @Column(name = "Bucket_Display_Name", nullable = true)
    private String Bucket_Display_Name;

    @Column(name = "RawScore", nullable = true)
    private double RawScore;

    @Column(name = "Probability", nullable = true)
    private double Probability;

    @Column(name = "Lift", nullable = true)
    private double Lift;
 
    @Column(name = "Percentile", nullable = true)
    private int Percentile;

    public static class ScoreOutputPK implements Serializable{

        private static final long serialVersionUID = 1L;

        @SuppressWarnings("unused")
        private String LeadId;

        @SuppressWarnings("unused")
        private String Play_Display_Name;

        public ScoreOutputPK (String leadId, String playDisplayName) {
            this.LeadId = leadId;
            this.Play_Display_Name = playDisplayName;
        }
    }

    public String getLeadId(){
        return LeadId;
    }

    public void setLeadId(String leadId){
        this.LeadId = leadId;
    }

    public String getPlay_Display_Name(){
        return Play_Display_Name;
    }

    public void setPlay_Display_Name(String playDisplayName){
        this.Play_Display_Name = playDisplayName;
    }

    public int getScore(){
        return this.Score;
    }

    public void setScore(int score){
        this.Score = score;
    }

    public String getBucketDisplayName(){
        return this.Bucket_Display_Name;
    }

    public void setBucketDisplayName(String bucketDisplayName){
        this.Bucket_Display_Name = bucketDisplayName;
    }

    public double getRawScore(){
        return this.RawScore;
    }

    public void setRawScore(double rawScore){
        this.RawScore = rawScore;
    }

    public double getProbability(){
        return this.Probability;
    }

    public void setProbability(double probability){
        this.Probability = probability;
    }

    public double getLift(){
        return this.Lift;
    }

    public void setLift(double lift){
        this.Lift = lift;
    }

    public int getPercentile(){
        return this.Percentile;
    }

    public void setPercentile(int percentile){
        this.Percentile = percentile;
    }
}
