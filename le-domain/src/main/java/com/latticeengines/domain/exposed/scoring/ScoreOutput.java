package com.latticeengines.domain.exposed.scoring;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.latticeengines.domain.exposed.dataplatform.HasId;

@Entity
@Table(name = "ScoreOutput")
public class ScoreOutput implements HasId<String>{

    private String leadId;

    private int score;

    private String bucketDisplayName;

    private String modelGUID;

    private float rawScore;

    private float probability;

    private float lift;
 
    private int percentile;

    @Id
    @Column(name = "LeadID", nullable = true)
    public String getId(){
        return leadId;
    }

    public void setId(String leadId){
        this.leadId = leadId;
    }

    @Column(name = "Score", nullable = true)
    public int getScore(){
        return this.score;
    }

    public void setScore(int score){
        this.score = score;
    }

    @Column(name = "Bucket_Display_Name", nullable = true)
    public String getBucketDisplayName(){
        return this.bucketDisplayName;
    }

    public void setBucketDisplayName(String bucketDisplayName){
        this.bucketDisplayName = bucketDisplayName;
    }

    @Column(name = "Play_Display_Name", nullable = true)
    public String getModelGUID(){
        return this.modelGUID;
    }
    
    public void setModelGUID(String modelGUID){
        this.modelGUID = modelGUID;
    }

    @Column(name = "RawScore", nullable = true)
    public float getRawScore(){
        return this.rawScore;
    }

    public void setRawScore(float rawScore){
        this.rawScore = rawScore;
    }

    @Column(name = "Probability", nullable = true)
    public float getProbability(){
        return this.probability;
    }

    public void setProbability(float probability){
        this.probability = probability;
    }

    @Column(name = "Lift", nullable = true)
    public float getLift(){
        return this.lift;
    }

    public void setLift(float lift){
        this.lift = lift;
    }

    @Column(name = "Percentile", nullable = true)
    public int getPercentile(){
        return this.percentile;
    }

    public void setPercentile(int percentile){
        this.percentile = percentile;
    }
}
