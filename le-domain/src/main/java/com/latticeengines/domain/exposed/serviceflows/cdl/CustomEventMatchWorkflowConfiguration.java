package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.modeling.ModelingType;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MatchStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ProcessMatchResultConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.BulkMatchWorkflowConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class CustomEventMatchWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.DataCloud.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    @JsonProperty("modelingType")
    private ModelingType modelingType;

    public ModelingType getModelingType() {
        return modelingType;
    }

    public void setModelingType(ModelingType modelingType) {
        this.modelingType = modelingType;
    }

    public static class Builder {
        private CustomEventMatchWorkflowConfiguration configuration = new CustomEventMatchWorkflowConfiguration();
        private MatchStepConfiguration matchStepConfiguration = new MatchStepConfiguration();
        private BulkMatchWorkflowConfiguration.Builder bulkMatchWorkflowConfigurationBuilder = new BulkMatchWorkflowConfiguration.Builder();
        private ProcessMatchResultConfiguration processMatchResultConfiguration = new ProcessMatchResultConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("customEventMatchingWorkflowConfiguration", customerSpace,
                    configuration.getClass().getSimpleName());

            return this;
        }

        public Builder modelingType(ModelingType modelingType) {
            configuration.setModelingType(modelingType);
            return this;
        }

        public CustomEventMatchWorkflowConfiguration build() {
            return configuration;
        }
    }
}
