package com.latticeengines.serviceflows.workflow.modeling;

import org.apache.commons.lang3.StringUtils;

public enum ProvenanceProperties {

    LP("swlib.artifact_id=le-serviceflows-leadprioritization"),
    PD("swlib.artifact_id=le-serviceflows-prospectdiscovery"),
    CG("swlib.artifact_id=le-serviceflows-customergrowth");
    
    private String artifactId;
    
    ProvenanceProperties(String artifactId) {
        this.artifactId = artifactId;
    }
    
    public String getResolvedProperties() {
        String[] props = new String[] { //
                artifactId, //
                "swlib.group_id=com.latticeengines", //
                "swlib.version=latest", //
                "swlib.module=dataflowapi" //
        };
        return StringUtils.join(props, " ");
    }
    
}
