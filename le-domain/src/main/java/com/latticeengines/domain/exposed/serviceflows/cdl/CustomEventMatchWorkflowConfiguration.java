package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.modeling.ModelingType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlMergeConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlSplitConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlStepConfiguration;
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

        private MatchCdlAccountWorkflowConfiguration.Builder matchAccountBuilder = new MatchCdlAccountWorkflowConfiguration.Builder();
        private MatchCdlStepConfiguration matchCdlStep = new MatchCdlStepConfiguration();
        private MatchCdlSplitConfiguration split = new MatchCdlSplitConfiguration();
        private MatchCdlMergeConfiguration merge = new MatchCdlMergeConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("customEventMatchWorkflow", customerSpace,
                    "customEventMatchWorkflow");

            matchAccountBuilder.customer(customerSpace);
            matchCdlStep.setCustomerSpace(customerSpace);
            split.setCustomerSpace(customerSpace);
            merge.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            matchAccountBuilder.microServiceHostPort(microServiceHostPort);
            matchCdlStep.setMicroServiceHostPort(microServiceHostPort);
            split.setMicroServiceHostPort(microServiceHostPort);
            merge.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            matchAccountBuilder.internalResourceHostPort(internalResourceHostPort);
            matchCdlStep.setInternalResourceHostPort(internalResourceHostPort);
            split.setInternalResourceHostPort(internalResourceHostPort);
            merge.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder matchInputTableName(String tableName) {
            matchAccountBuilder.matchInputTableName(tableName);
            matchCdlStep.setMatchInputTableName(tableName);
            return this;
        }

        public Builder modelingType(ModelingType modelingType) {
            configuration.setModelingType(modelingType);
            return this;
        }

        public Builder matchRequestSource(MatchRequestSource matchRequestSource) {
            matchAccountBuilder.matchRequestSource(matchRequestSource);
            return this;
        }

        public Builder matchQueue(String queue) {
            matchAccountBuilder.matchQueue(queue);
            return this;
        }

        public Builder matchColumnSelection(Predefined predefinedColumnSelection, String selectionVersion) {
            matchAccountBuilder.matchColumnSelection(predefinedColumnSelection, selectionVersion);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            matchAccountBuilder.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchClientDocument(MatchClientDocument matchClientDocument) {
            matchAccountBuilder.matchClientDocument(matchClientDocument);
            return this;
        }

        public Builder matchType(MatchCommandType matchCommandType) {
            matchAccountBuilder.matchType(matchCommandType);
            return this;
        }

        public Builder setRetainLatticeAccountId(boolean retainLatticeAccountId) {
            matchAccountBuilder.setRetainLatticeAccountId(retainLatticeAccountId);
            return this;
        }

        public Builder matchDestTables(String destTables) {
            matchAccountBuilder.matchDestTables(destTables);
            return this;
        }

        public Builder excludePublicDomains(boolean excludePublicDomains) {
            matchAccountBuilder.excludePublicDomains(excludePublicDomains);
            return this;
        }

        public Builder excludeDataCloudAttrs(boolean exclude) {
            matchAccountBuilder.excludeDataCloudAttrs(exclude);
            return this;
        }

        public Builder skipDedupStep(boolean skipDedupStep) {
            matchAccountBuilder.skipDedupStep(skipDedupStep);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            matchAccountBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public CustomEventMatchWorkflowConfiguration build() {
            configuration.add(matchAccountBuilder.build());
            configuration.add(matchCdlStep);
            configuration.add(split);
            configuration.add(merge);

            return configuration;
        }
    }
}
