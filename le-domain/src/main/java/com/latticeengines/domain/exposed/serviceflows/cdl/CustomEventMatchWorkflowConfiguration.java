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

        private MatchCdlWithAccountIdWorkflowConfiguration.Builder matchWithAccountIdBuilder = new MatchCdlWithAccountIdWorkflowConfiguration.Builder();
        private MatchCdlWithoutAccountIdWorkflowConfiguration.Builder matchWithoutAccountIdBuilder = new MatchCdlWithoutAccountIdWorkflowConfiguration.Builder();
        private MatchCdlStepConfiguration matchCdlStep = new MatchCdlStepConfiguration();
        private MatchCdlSplitConfiguration split = new MatchCdlSplitConfiguration();
        private MatchCdlMergeConfiguration merge = new MatchCdlMergeConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("customEventMatchWorkflow", customerSpace,
                    "customEventMatchWorkflow");

            matchWithAccountIdBuilder.customer(customerSpace);
            matchWithoutAccountIdBuilder.customer(customerSpace);
            split.setCustomerSpace(customerSpace);
            merge.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            matchWithAccountIdBuilder.microServiceHostPort(microServiceHostPort);
            matchWithoutAccountIdBuilder.microServiceHostPort(microServiceHostPort);
            split.setMicroServiceHostPort(microServiceHostPort);
            merge.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            matchWithAccountIdBuilder.internalResourceHostPort(internalResourceHostPort);
            matchWithoutAccountIdBuilder.internalResourceHostPort(internalResourceHostPort);
            split.setInternalResourceHostPort(internalResourceHostPort);
            merge.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder matchInputTableName(String tableName) {
            matchWithAccountIdBuilder.matchInputTableName(tableName);
            matchWithoutAccountIdBuilder.matchInputTableName(tableName);
            return this;
        }

        public Builder modelingType(ModelingType modelingType) {
            configuration.setModelingType(modelingType);
            return this;
        }

        public Builder matchRequestSource(MatchRequestSource matchRequestSource) {
            matchWithAccountIdBuilder.matchRequestSource(matchRequestSource);
            matchWithoutAccountIdBuilder.matchRequestSource(matchRequestSource);

            return this;
        }

        public Builder matchQueue(String queue) {
            matchWithAccountIdBuilder.matchQueue(queue);
            matchWithoutAccountIdBuilder.matchQueue(queue);
            return this;
        }

        public Builder matchColumnSelection(Predefined predefinedColumnSelection, String selectionVersion) {
            matchWithAccountIdBuilder.matchColumnSelection(predefinedColumnSelection, selectionVersion);
            matchWithoutAccountIdBuilder.matchColumnSelection(predefinedColumnSelection, selectionVersion);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            matchWithAccountIdBuilder.dataCloudVersion(dataCloudVersion);
            matchWithoutAccountIdBuilder.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchClientDocument(MatchClientDocument matchClientDocument) {
            matchWithAccountIdBuilder.matchClientDocument(matchClientDocument);
            matchWithoutAccountIdBuilder.matchClientDocument(matchClientDocument);
            return this;
        }

        public Builder matchType(MatchCommandType matchCommandType) {
            matchWithAccountIdBuilder.matchType(matchCommandType);
            matchWithoutAccountIdBuilder.matchType(matchCommandType);
            return this;
        }

        public Builder setRetainLatticeAccountId(boolean retainLatticeAccountId) {
            matchWithAccountIdBuilder.setRetainLatticeAccountId(retainLatticeAccountId);
            matchWithoutAccountIdBuilder.setRetainLatticeAccountId(retainLatticeAccountId);
            return this;
        }

        public Builder matchDestTables(String destTables) {
            matchWithAccountIdBuilder.matchDestTables(destTables);
            matchWithoutAccountIdBuilder.matchDestTables(destTables);
            return this;
        }

        public Builder excludePublicDomains(boolean excludePublicDomains) {
            matchWithAccountIdBuilder.excludePublicDomains(excludePublicDomains);
            matchWithoutAccountIdBuilder.excludePublicDomains(excludePublicDomains);
            return this;
        }

        public Builder excludeDataCloudAttrs(boolean exclude) {
            matchWithAccountIdBuilder.excludeDataCloudAttrs(exclude);
            matchWithoutAccountIdBuilder.excludeDataCloudAttrs(exclude);
            return this;
        }

        public Builder skipDedupStep(boolean skipDedupStep) {
            matchWithAccountIdBuilder.skipDedupStep(skipDedupStep);
            matchWithoutAccountIdBuilder.skipDedupStep(skipDedupStep);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            matchWithAccountIdBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            matchWithoutAccountIdBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public CustomEventMatchWorkflowConfiguration build() {
            configuration.add(matchWithAccountIdBuilder.build());
            configuration.add(matchWithoutAccountIdBuilder.build());
            configuration.add(matchCdlStep);
            configuration.add(split);
            configuration.add(merge);

            return configuration;
        }
    }
}
