package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.Collection;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class GenerateRatingWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Scoring.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {

        private GenerateRatingWorkflowConfiguration configuration = new GenerateRatingWorkflowConfiguration();
        private GenerateAIRatingWorkflowConfiguration.Builder generateAIRatingWorkflowBuilder = new GenerateAIRatingWorkflowConfiguration.Builder();
        private GenerateRatingStepConfiguration generateRatingStepConfiguration = new GenerateRatingStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            generateRatingStepConfiguration.setCustomerSpace(customerSpace);
            generateAIRatingWorkflowBuilder.customer(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            generateRatingStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            generateAIRatingWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder dataCloudVersion(DataCloudVersion dataCloudVersion) {
            generateAIRatingWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchYarnQueue(String matchYarnQueue) {
            generateAIRatingWorkflowBuilder.matchYarnQueue(matchYarnQueue);
            return this;
        }

        public Builder fetchOnly(boolean fetchOnly) {
            generateAIRatingWorkflowBuilder.fetchOnly(fetchOnly);
            return this;
        }

        public GenerateRatingWorkflowConfiguration build() {
            configuration.setContainerConfiguration("generateRatingWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(generateRatingStepConfiguration);
            configuration.add(generateAIRatingWorkflowBuilder.build());
            return configuration;
        }
    }
}
