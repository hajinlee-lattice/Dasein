package com.latticeengines.domain.exposed.dante.metadata;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Association extends BaseObjectMetadata implements Serializable {
    @JsonProperty("Cardinality")
    private int cardinality;

    @JsonProperty("CardinalityString")
    private String cardinalityString;

    @JsonProperty("SourceKeyName")
    private String sourceKeyName;

    @JsonProperty("TargetKeyName")
    private String targetKeyName;

    @JsonProperty("TargetNotion")
    private String targetNotion;

    public int getCardinality() {
        return cardinality;
    }

    public void setCardinality(int cardinality) {
        this.cardinality = cardinality;
    }

    public String getCardinalityString() {
        return cardinalityString;
    }

    public void setCardinalityString(String cardinalityString) {
        this.cardinalityString = cardinalityString;
    }

    public String getSourceKeyName() {
        return sourceKeyName;
    }

    public void setSourceKeyName(String sourceKeyName) {
        this.sourceKeyName = sourceKeyName;
    }

    public String getTargetKeyName() {
        return targetKeyName;
    }

    public void setTargetKeyName(String targetKeyName) {
        this.targetKeyName = targetKeyName;
    }

    public String getTargetNotion() {
        return targetNotion;
    }

    public void setTargetNotion(String targetNotion) {
        this.targetNotion = targetNotion;
    }
}
