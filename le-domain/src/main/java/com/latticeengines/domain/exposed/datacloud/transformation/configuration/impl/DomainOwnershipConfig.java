package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DomainOwnershipConfig extends TransformerConfig {

    public final static String ROOT_DUNS = "ROOT_DUNS";
    public final static String DUNS_TYPE = "DUNS_TYPE";
    public final static String TREE_NUMBER = "TREE_NUMBER";
    public final static String REASON_TYPE = "REASON_TYPE";

    @JsonProperty("FranchiseThreshold")
    private int franchiseThreshold;

    @JsonProperty("MultLargeCompThreshold")
    private Long multLargeCompThreshold;

    public int getFranchiseThreshold() {
        return franchiseThreshold;
    }

    public void setFranchiseThreshold(int franchiseThreshold) {
        this.franchiseThreshold = franchiseThreshold;
    }

    public Long getMultLargeCompThreshold() {
        return multLargeCompThreshold;
    }

    public void setMultLargeCompThreshold(Long multLargeCompThreshold) {
        this.multLargeCompThreshold = multLargeCompThreshold;
    }

}
