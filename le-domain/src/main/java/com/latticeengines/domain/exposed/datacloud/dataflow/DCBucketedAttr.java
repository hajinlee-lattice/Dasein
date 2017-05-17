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

    public DCBucketedAttr(String nominalAttr, int lowestBit, int numBits) {
        super(nominalAttr, new ArrayList<>(), lowestBit, numBits);
    }

    @JsonProperty("decode_strategy")
    private BitDecodeStrategy decodedStrategy;

    @JsonProperty("bucket_algo")
    private BucketAlgorithm bucketAlgo;

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
        if ((getBuckets() == null || getBuckets().isEmpty())
                && (bucketAlgo != null && bucketAlgo.generateLabels() != null)) {
            setBuckets(bucketAlgo.generateLabels());
        }
    }

}
