package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FormDomOwnershipTableConfig extends TransformerConfig {

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
