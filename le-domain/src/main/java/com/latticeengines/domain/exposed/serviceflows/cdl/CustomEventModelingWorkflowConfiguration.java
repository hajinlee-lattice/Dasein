package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.modeling.ModelingType;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class CustomEventModelingWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Modeling.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {
        private CustomEventModelingWorkflowConfiguration configuration = new CustomEventModelingWorkflowConfiguration();

        private CustomEventMatchWorkflowConfiguration.Builder customEventMatchWorkflowConfigurationBuilder = new CustomEventMatchWorkflowConfiguration.Builder();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("customEventModelingWorkflowConfiguration", customerSpace,
                    configuration.getClass().getSimpleName());

            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            return this;
        }

        public Builder modelingType(ModelingType modelingType) {
            customEventMatchWorkflowConfigurationBuilder.modelingType(modelingType);
            return this;
        }

        public CustomEventModelingWorkflowConfiguration build() {
            configuration.add(customEventMatchWorkflowConfigurationBuilder.build());
            return configuration;
        }
    }
}
