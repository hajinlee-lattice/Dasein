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
    private float RawScore;

    @Column(name = "Probability", nullable = true)
    private float Probability;

    @Column(name = "Lift", nullable = true)
    private float Lift;
 
    @Column(name = "Percentile", nullable = true)
    private int Percentile;

    public static class ScoreOutputPK implements Serializable{

        private static final long serialVersionUID = 1L;

        @SuppressWarnings("unused")
        private String LeadId;

        @SuppressWarnings("unused")
        private String Play_Display_Name;

        public ScoreOutputPK (String LeadId, String Play_Display_Name) {
            this.LeadId = LeadId;
            this.Play_Display_Name = Play_Display_Name;
        }
    }

    public String getLeadId(){
        return LeadId;
    }

    public void setLeadId(String LeadId){
        this.LeadId = LeadId;
    }

    public String getPlay_Display_Name(){
        return Play_Display_Name;
    }

    public void setPlay_Display_Name(String Play_Display_Name){
        this.Play_Display_Name = Play_Display_Name;
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

    public float getRawScore(){
        return this.RawScore;
    }

    public void setRawScore(float rawScore){
        this.RawScore = rawScore;
    }

    public float getProbability(){
        return this.Probability;
    }

    public void setProbability(float probability){
        this.Probability = probability;
    }

    public float getLift(){
        return this.Lift;
    }

    public void setLift(float lift){
        this.Lift = lift;
    }

    public int getPercentile(){
        return this.Percentile;
    }

    public void setPercentile(int percentile){
        this.Percentile = percentile;
    }
}
