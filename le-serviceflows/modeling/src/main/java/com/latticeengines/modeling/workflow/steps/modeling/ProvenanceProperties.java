package com.latticeengines.modeling.workflow.steps.modeling;

import org.apache.commons.lang3.StringUtils;

public enum ProvenanceProperties {

    LP("swlib.artifact_id=le-serviceflows-leadprioritization"),
    CG("swlib.artifact_id=le-serviceflows-customergrowth");

    private String artifactId;

    ProvenanceProperties(String artifactId) {
        this.artifactId = artifactId;
    }

    public String getResolvedProperties() {
        String[] props = new String[] { //
                artifactId, //
                "swlib.group_id=com.latticeengines", //
                "ledp.version=latest", //
                "leds.version=latest", //
                "swlib.module=dataflowapi" //
        };
        return StringUtils.join(props, " ");
    }

}
