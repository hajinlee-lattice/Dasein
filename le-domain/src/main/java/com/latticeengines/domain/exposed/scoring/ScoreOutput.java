package com.latticeengines.domain.exposed.scoring;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.apache.avro.Schema;

@Entity
public class ScoreOutput extends org.apache.avro.specific.SpecificRecordBase implements
        org.apache.avro.specific.SpecificRecord {

    @Id
    @Column(name = "LeadID", nullable = true)
    private String LeadID;

    @Column(name = "Play_Display_Name", nullable = true)
    private String Play_Display_Name;

    @Column(name = "Score", nullable = true)
    private Integer Score;

    @Column(name = "Bucket_Display_Name", nullable = true)
    private String Bucket_Display_Name;

    @Column(name = "RawScore", nullable = true)
    private Double RawScore;

    @Column(name = "Probability", nullable = true)
    private Double Probability;

    @Column(name = "Lift", nullable = true)
    private Double Lift;

    @Column(name = "Percentile", nullable = true)
    private Integer Percentile;

    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser()
            .parse("{\"type\":\"record\",\"name\":\"ScoreOutput\",\"namespace\":\"com.latticeengines.domain.exposed.scoring\",\"fields\":[{\"name\":\"LeadID\",\"type\":[\"string\",\"null\"],\"columnName\":\"LeadID\",\"sqlType\":\"12\"},{\"name\":\"Bucket_Display_Name\",\"type\":[\"string\",\"null\"],\"columnName\":\"Bucket_Display_Name\",\"sqlType\":\"12\"},{\"name\":\"Lift\",\"type\":[\"double\",\"null\"],\"columnName\":\"Lift\",\"sqlType\":\"7\"},{\"name\":\"Play_Display_Name\",\"type\":[\"string\",\"null\"],\"columnName\":\"Play_Display_Name\",\"sqlType\":\"12\"},{\"name\":\"Percentile\",\"type\":[\"int\",\"null\"],\"columnName\":\"Percentile\",\"sqlType\":\"4\"},{\"name\":\"Probability\",\"type\":[\"double\",\"null\"],\"columnName\":\"Probability\",\"sqlType\":\"7\"},{\"name\":\"RawScore\",\"type\":[\"double\",\"null\"],\"columnName\":\"RawScore\",\"sqlType\":\"7\"},{\"name\":\"Score\",\"type\":[\"int\",\"null\"],\"columnName\":\"Score\",\"sqlType\":\"4\"}]}");

    public static org.apache.avro.Schema getClassSchema() {
        return SCHEMA$;
    }

    public ScoreOutput() {
    }

    public ScoreOutput(String leadID, String bucketDisplayName, Double lift, String playDisplayName, Integer percentile,
            Double probability, Double rawScore, Integer score) {
        this.LeadID = leadID;
        this.Play_Display_Name = playDisplayName;
        this.Bucket_Display_Name = bucketDisplayName;
        this.RawScore = rawScore;
        this.Probability = probability;
        this.Lift = lift;
        this.Score = score;
        this.Percentile = percentile;
    }

    public String getLeadID() {
        return LeadID;
    }

    public void setLeadID(String leadID) {
        this.LeadID = leadID;
    }

    public String getPlay_Display_Name() {
        return Play_Display_Name;
    }

    public void setPlay_Display_Name(String playDisplayName) {
        this.Play_Display_Name = playDisplayName;
    }

    public Integer getScore() {
        return this.Score;
    }

    public void setScore(Integer score) {
        this.Score = score;
    }

    public String getBucketDisplayName() {
        return this.Bucket_Display_Name;
    }

    public void setBucketDisplayName(String bucketDisplayName) {
        this.Bucket_Display_Name = bucketDisplayName;
    }

    public Double getRawScore() {
        return this.RawScore;
    }

    public void setRawScore(Double rawScore) {
        this.RawScore = rawScore;
    }

    public Double getProbability() {
        return this.Probability;
    }

    public void setProbability(Double probability) {
        this.Probability = probability;
    }

    public Double getLift() {
        return this.Lift;
    }

    public void setLift(Double lift) {
        this.Lift = lift;
    }

    public Integer getPercentile() {
        return this.Percentile;
    }

    public void setPercentile(Integer percentile) {
        this.Percentile = percentile;
    }

    @Override
    public Object get(int field$) {
        switch (field$) {
        case 0:
            return LeadID;
        case 1:
            return Bucket_Display_Name;
        case 2:
            return Lift;
        case 3:
            return Play_Display_Name;
        case 4:
            return Percentile;
        case 5:
            return Probability;
        case 6:
            return RawScore;
        case 7:
            return Score;
        default:
            throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    @Override
    public Schema getSchema() {
        return SCHEMA$;
    }

    @Override
    public void put(int field$, Object value$) {
        switch (field$) {
        case 0:
            LeadID = String.valueOf(value$);
            break;
        case 1:
            Bucket_Display_Name = String.valueOf(value$);
            break;
        case 2:
            Lift = (java.lang.Double) value$;
            break;
        case 3:
            Play_Display_Name = String.valueOf(value$);
            break;
        case 4:
            Percentile = (java.lang.Integer) value$;
            break;
        case 5:
            Probability = (java.lang.Double) value$;
            break;
        case 6:
            RawScore = (java.lang.Double) value$;
            break;
        case 7:
            Score = (java.lang.Integer) value$;
            break;
        default:
            throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }
}
