package com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class AttributeCategoryModifierConfiguration extends MicroserviceStepConfiguration {

    private boolean excludeDataCloudAttributes;

    private boolean excludeCDLAttributes;

    public boolean isExcludeDataCloudAttributes() {
        return excludeDataCloudAttributes;
    }

    public void setExcludeDataCloudAttributes(boolean excludeDataCloudAttributes) {
        this.excludeDataCloudAttributes = excludeDataCloudAttributes;
    }

    public boolean isExcludeCDLAttributes() {
        return excludeCDLAttributes;
    }

    public void setExcludeCDLAttributes(boolean excludeCDLAttributes) {
        this.excludeCDLAttributes = excludeCDLAttributes;
    }

    public static class Builder {

        private CustomerSpace customerSpace;

        private boolean excludeDataCloudAttributes;

        private boolean excludeCDLAttributes;

        private boolean skipStep;

        public AttributeCategoryModifierConfiguration build() {
            AttributeCategoryModifierConfiguration configuration = new AttributeCategoryModifierConfiguration();
            configuration.setCustomerSpace(customerSpace);
            configuration.setExcludeDataCloudAttributes(excludeDataCloudAttributes);
            configuration.setExcludeCDLAttributes(excludeCDLAttributes);
            configuration.setSkipStep(skipStep);
            return configuration;
        }

        public Builder customerSpace(CustomerSpace customerSpace) {
            this.customerSpace = customerSpace;
            return this;
        }

        public Builder excludeDataCloudAttributes(boolean excludeDataCloudAttributes) {
            this.excludeDataCloudAttributes = excludeDataCloudAttributes;
            return this;
        }

        public Builder excludeCDLAttributes(boolean excludeCDLAttributes) {
            this.excludeCDLAttributes = excludeCDLAttributes;
            return this;
        }

        public Builder skipStep(boolean skipStep) {
            this.skipStep = skipStep;
            return this;
        }
    }
}
