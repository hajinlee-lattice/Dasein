package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

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
        private GenerateAIRatingWorkflowConfiguration.Builder generateAIRating = new GenerateAIRatingWorkflowConfiguration.Builder();
        private GenerateRatingStepConfiguration ratingStep = new GenerateRatingStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            ratingStep.setCustomerSpace(customerSpace);
            generateAIRating.customer(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            ratingStep.setMicroServiceHostPort(microServiceHostPort);
            generateAIRating.microServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            generateAIRating.internalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder dataCloudVersion(DataCloudVersion dataCloudVersion) {
            generateAIRating.dataCloudVersion(dataCloudVersion.getVersion());
            return this;
        }

        public Builder matchYarnQueue(String matchYarnQueue) {
            generateAIRating.matchYarnQueue(matchYarnQueue);
            return this;
        }

        public Builder useAccountFeature(boolean useAccountFeature) {
            generateAIRating.useAccountFeature(useAccountFeature);
            return this;
        }

        public Builder fetchOnly(boolean fetchOnly) {
            generateAIRating.fetchOnly(fetchOnly);
            return this;
        }

        public Builder uniqueKeyColumn(String uniqueKeyColumn) {
            generateAIRating.uniqueKeyColumn(uniqueKeyColumn);
            return this;
        }

        public Builder matchGroupId(String matchGroupId) {
            generateAIRating.matchGroupId(matchGroupId);
            generateAIRating.matchJoinInternalId(false);
            return this;
        }

        public Builder setUseScorederivation(boolean useScorederivation) {
            generateAIRating.setUseScorederivation(useScorederivation);
            return this;
        }

        public Builder cdlMultiModel(boolean cdlMultiMode) {
            generateAIRating.cdlMultiModel(cdlMultiMode);
            return this;
        }

        public Builder transformationGroup(TransformationGroup transformationGroup,
                List<TransformDefinition> stdTransformDefns) {
            generateAIRating.transformationGroup(transformationGroup, stdTransformDefns);
            return this;
        }

        public Builder forceEVSteps(boolean forceEVSteps) {
            generateAIRating.forceEVSteps(forceEVSteps);
            return this;
        }

        public Builder targetScoreDerivationEnabled(boolean targetScoreDerivationEnabled) {
            generateAIRating.targetScoreDerivationEnabled(targetScoreDerivationEnabled);
            return this;
        }

        public Builder apsRollupPeriod(String period) {
            generateAIRating.apsRollupPeriod(period);
            return this;
        }

        public GenerateRatingWorkflowConfiguration build() {
            configuration.setContainerConfiguration("generateRatingWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(ratingStep);
            configuration.add(generateAIRating.build());
            return configuration;
        }
    }
}
