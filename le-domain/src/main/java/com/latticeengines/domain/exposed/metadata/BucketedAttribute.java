package com.latticeengines.domain.exposed.metadata;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BucketedAttribute implements Serializable {

    private static final long serialVersionUID = -1967674185815017667L;

    @JsonProperty("nominal_attr")
    private String nominalAttr;

    @JsonProperty("buckets")
    private List<String> buckets;

    @JsonProperty("lowest_bit")
    private int lowestBit;

    @JsonProperty("num_bits")
    private int numBits;

    // for jackson
    protected BucketedAttribute() {}

    public BucketedAttribute(String nominalAttr, List<String> buckets, int lowestBit, int numBits) {
        this.nominalAttr = nominalAttr;
        this.buckets = buckets;
        this.lowestBit = lowestBit;
        this.numBits = numBits;
    }

    public String getNominalAttr() {
        return nominalAttr;
    }

    public List<String> getBuckets() {
        return buckets;
    }

    public void setBuckets(List<String> buckets) {
        this.buckets = buckets;
    }

    public int getLowestBit() {
        return lowestBit;
    }

    public int getNumBits() {
        return numBits;
    }
}
