package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.io.Serializable;
import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.BucketedAttribute;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DCBucketedAttr extends BucketedAttribute implements Serializable {

    private static final long serialVersionUID = -1L;

    // for jackson
    private DCBucketedAttr() {
    }

    public DCBucketedAttr(String nominalAttr, String sourceAttr, int lowestBit, int numBits) {
        super(nominalAttr, new ArrayList<>(), lowestBit, numBits);
        this.sourceAttr = sourceAttr;
    }

    @JsonProperty("dec_strat")
    private BitDecodeStrategy decodedStrategy;

    @JsonProperty("bkt_algo")
    private BucketAlgorithm bucketAlgo;

    @JsonProperty("src_attr")
    private String sourceAttr;

    public BitDecodeStrategy getDecodedStrategy() {
        return decodedStrategy;
    }

    public void setDecodedStrategy(BitDecodeStrategy decodedStrategy) {
        this.decodedStrategy = decodedStrategy;
    }

    public BucketAlgorithm getBucketAlgo() {
        return bucketAlgo;
    }

    public void setBucketAlgo(BucketAlgorithm bucketAlgo) {
        this.bucketAlgo = bucketAlgo;
    }

    public String getSourceAttr() {
        return sourceAttr;
    }

    public void setSourceAttr(String sourceAttr) {
        this.sourceAttr = sourceAttr;
    }
}
