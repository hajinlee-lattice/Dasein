package com.latticeengines.domain.exposed.metadata;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BucketedAttribute {

    @JsonProperty("nominal_attr")
    private String nominalAttr;

    @JsonProperty("buckets")
    private List<String> buckets;

    @JsonProperty("lowest_bit")
    private int lowestBit;

    @JsonProperty("num_bits")
    private int numBits;

    // for jacksoon
    private BucketedAttribute() {

    }

    public BucketedAttribute(String nominalAttr, List<String> buckets, int lowestBit, int numBits) {
        this.nominalAttr = nominalAttr;
        this.buckets = buckets;
        this.lowestBit = lowestBit;
        this.numBits = numBits;
    }

    public String getNominalAttr() {
        return nominalAttr;
    }

    private void setNominalAttr(String nominalAttr) {
        this.nominalAttr = nominalAttr;
    }

    public List<String> getBuckets() {
        return buckets;
    }

    private void setBuckets(List<String> buckets) {
        this.buckets = buckets;
    }

    public int getLowestBit() {
        return lowestBit;
    }

    private void setLowestBit(int lowestBit) {
        this.lowestBit = lowestBit;
    }

    public int getNumBits() {
        return numBits;
    }

    private void setNumBits(int numBits) {
        this.numBits = numBits;
    }
}
