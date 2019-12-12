package com.latticeengines.domain.exposed.datacloud.transformation.config.ams;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

public class AMSeedMarkerConfig extends TransformerConfig {
    @JsonProperty("SrcPriorityToMrkPriDom")
    private String[] srcPriorityToMrkPriDom;

    @JsonProperty("GoldenDomSrcs")
    private String[] goldenDomSrcs;

    public String[] getSrcPriorityToMrkPriDom() {
        return srcPriorityToMrkPriDom;
    }

    public void setSrcPriorityToMrkPriDom(String[] srcPriorityToMrkPriDom) {
        this.srcPriorityToMrkPriDom = srcPriorityToMrkPriDom;
    }

    public String[] getGoldenDomSrcs() {
        return goldenDomSrcs;
    }

    public void setGoldenDomSrcs(String[] goldenDomSrcs) {
        this.goldenDomSrcs = goldenDomSrcs;
    }

}
