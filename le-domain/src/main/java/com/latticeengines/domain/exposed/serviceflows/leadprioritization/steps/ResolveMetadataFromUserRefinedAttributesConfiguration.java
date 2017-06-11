package com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class ResolveMetadataFromUserRefinedAttributesConfiguration extends MicroserviceStepConfiguration {

    private List<Attribute> userRefinedAttributes;

    public List<Attribute> getUserRefinedAttributes() {
        return this.userRefinedAttributes;
    }

    public void setUserRefinedAttributes(List<Attribute> userRefinedAttributes) {
        this.userRefinedAttributes = userRefinedAttributes;
    }
}
