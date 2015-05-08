package com.latticeengines.domain.exposed.scoring;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "ScoreOutput")
public class ScoreOutput{

    @EmbeddedId
    private ScoreOutputPK scoreOutputPK;

    @Column(name = "Request_ID", nullable = false)
    private String requestId;

    @Column(name = "Score", nullable = true)
    private int score;

    @Column(name = "Bucket_Display_Name", nullable = true)
    private String bucketDisplayName;

    @Column(name = "RawScore", nullable = true)
    private float rawScore;

    @Column(name = "Probability", nullable = true)
    private float probability;

    @Column(name = "Lift", nullable = true)
    private float lift;
 
    @Column(name = "Percentile", nullable = true)
    private int percentile;
    

     
    public ScoreOutputPK getPK() {
        return scoreOutputPK;
    }
    
    public void setScoreOutputPK(ScoreOutputPK scoreOutputPK){
        this.scoreOutputPK = scoreOutputPK;
    }
    
    @Embeddable
    public static class ScoreOutputPK implements Serializable {

        private static final long serialVersionUID = 1L;
        @Column(name = "LeadID", nullable = true)
        String leadId;

        @Column(name = "Play_Display_Name", nullable = true)
        String modelGUID;
        
        public ScoreOutputPK () {}
        
        public ScoreOutputPK (String leadId, String modelGUID) {
            this.leadId = leadId;
            this.modelGUID = modelGUID;
        }
    }

    public int getScore(){
        return this.score;
    }

    public void setScore(int score){
        this.score = score;
    }

    public String getBucketDisplayName(){
        return this.bucketDisplayName;
    }

    public void setBucketDisplayName(String bucketDisplayName){
        this.bucketDisplayName = bucketDisplayName;
    }

    public float getRawScore(){
        return this.rawScore;
    }

    public void setRawScore(float rawScore){
        this.rawScore = rawScore;
    }

    public float getProbability(){
        return this.probability;
    }

    public void setProbability(float probability){
        this.probability = probability;
    }

    public float getLift(){
        return this.lift;
    }

    public void setLift(float lift){
        this.lift = lift;
    }

    public int getPercentile(){
        return this.percentile;
    }

    public void setPercentile(int percentile){
        this.percentile = percentile;
    }
}
