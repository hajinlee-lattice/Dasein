package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AMSeedMarkerConfig extends TransformerConfig {
    @JsonProperty("SrcPriorityToMrkPriDom")
    private String[] srcPriorityToMrkPriDom;

    public String[] getSrcPriorityToMrkPriDom() {
        return srcPriorityToMrkPriDom;
    }

    public void setSrcPriorityToMrkPriDom(String[] srcPriorityToMrkPriDom) {
        this.srcPriorityToMrkPriDom = srcPriorityToMrkPriDom;
    }

}
