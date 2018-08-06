package com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps;

import java.util.Map;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class MergeUserRefinedAttributesConfiguration extends MicroserviceStepConfiguration {

    private Map<String, ColumnMetadata> userRefinedAttributes;

    public Map<String, ColumnMetadata> getUserRefinedAttributes() {
        return this.userRefinedAttributes;
    }

    public void setUserRefinedAttributes(Map<String, ColumnMetadata> userRefinedAttributes) {
        this.userRefinedAttributes = userRefinedAttributes;
    }
}
