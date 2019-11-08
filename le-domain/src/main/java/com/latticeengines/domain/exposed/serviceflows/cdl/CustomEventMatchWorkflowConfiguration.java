package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlAccountConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlMergeConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlSplitConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MatchStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.MatchDataCloudWorkflowConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class CustomEventMatchWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    @JsonProperty("customEventModelingType")
    private CustomEventModelingType customEventModelingType;

    @JsonProperty("cdlEntityType")
    private EntityType cdlEntityType = EntityType.Accounts;

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.DataCloud.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public CustomEventModelingType getCustomEventModelingType() {
        return customEventModelingType;
    }

    public void setCustomEventModelingType(CustomEventModelingType customEventModelingType) {
        this.customEventModelingType = customEventModelingType;
    }

    public EntityType getCdlEntityType() {
        return cdlEntityType;
    }

    public void setCdlEntityType(EntityType cdlEntityType) {
        this.cdlEntityType = cdlEntityType;
    }

    public static class Builder {
        private CustomEventMatchWorkflowConfiguration configuration = new CustomEventMatchWorkflowConfiguration();

        private MatchDataCloudWorkflowConfiguration.Builder matchDataCloudWorkflowBuilder = new MatchDataCloudWorkflowConfiguration.Builder();

        private MatchCdlAccountWorkflowConfiguration.Builder matchAccountWithIdBuilder = new MatchCdlAccountWorkflowConfiguration.Builder();
        private MatchCdlAccountWorkflowConfiguration.Builder matchAccountWithoutIdBuilder = new MatchCdlAccountWorkflowConfiguration.Builder();
        private MatchCdlAccountConfiguration matchCdlAccount = new MatchCdlAccountConfiguration();
        private MatchCdlStepConfiguration matchCdlStep = new MatchCdlStepConfiguration();
        private MatchCdlSplitConfiguration split = new MatchCdlSplitConfiguration();
        private MatchCdlMergeConfiguration merge = new MatchCdlMergeConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            matchAccountWithIdBuilder.customer(customerSpace);
            matchAccountWithoutIdBuilder.customer(customerSpace);
            matchCdlAccount.setCustomerSpace(customerSpace);
            matchCdlStep.setCustomerSpace(customerSpace);
            split.setCustomerSpace(customerSpace);
            merge.setCustomerSpace(customerSpace);
            matchDataCloudWorkflowBuilder.customer(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            matchAccountWithIdBuilder.microServiceHostPort(microServiceHostPort);
            matchAccountWithoutIdBuilder.microServiceHostPort(microServiceHostPort);
            matchCdlAccount.setMicroServiceHostPort(microServiceHostPort);
            matchCdlStep.setMicroServiceHostPort(microServiceHostPort);
            split.setMicroServiceHostPort(microServiceHostPort);
            merge.setMicroServiceHostPort(microServiceHostPort);
            matchDataCloudWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            matchAccountWithIdBuilder.internalResourceHostPort(internalResourceHostPort);
            matchAccountWithoutIdBuilder.internalResourceHostPort(internalResourceHostPort);
            matchCdlAccount.setInternalResourceHostPort(internalResourceHostPort);
            matchCdlStep.setInternalResourceHostPort(internalResourceHostPort);
            split.setInternalResourceHostPort(internalResourceHostPort);
            merge.setInternalResourceHostPort(internalResourceHostPort);
            matchDataCloudWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder matchInputTableName(String tableName) {
            matchAccountWithIdBuilder.matchInputTableName(tableName);
            matchAccountWithoutIdBuilder.matchInputTableName(tableName);
            matchCdlAccount.setMatchInputTableName(tableName);
            matchCdlStep.setMatchInputTableName(tableName);
            matchDataCloudWorkflowBuilder.matchInputTableName(tableName);
            return this;
        }

        public Builder matchAccountIdColumn(String matchAccountIdColumn) {
            matchCdlAccount.setMatchAccountIdColumn(matchAccountIdColumn);
            return this;
        }

        public Builder modelingType(CustomEventModelingType customEventModelingType) {
            configuration.setCustomEventModelingType(customEventModelingType);
            return this;
        }

        public Builder cdlEntityType(EntityType cdlEntityType) {
            configuration.setCdlEntityType(cdlEntityType);
            return this;
        }

        public Builder matchRequestSource(MatchRequestSource matchRequestSource) {
            matchAccountWithIdBuilder.matchRequestSource(matchRequestSource);
            matchAccountWithoutIdBuilder.matchRequestSource(matchRequestSource);
            matchDataCloudWorkflowBuilder.matchRequestSource(matchRequestSource);
            return this;
        }

        public Builder matchQueue(String queue) {
            matchAccountWithIdBuilder.matchQueue(queue);
            matchAccountWithoutIdBuilder.matchQueue(queue);
            matchDataCloudWorkflowBuilder.matchQueue(queue);
            return this;
        }

        public Builder fetchOnly(boolean fetchOnly) {
            matchAccountWithIdBuilder.fetchOnly(fetchOnly);
            return this;
        }

        public Builder matchColumnSelection(Predefined predefinedColumnSelection,
                String selectionVersion) {
            matchDataCloudWorkflowBuilder.matchColumnSelection(predefinedColumnSelection,
                    selectionVersion);
            matchAccountWithIdBuilder.matchColumnSelection(predefinedColumnSelection,
                    selectionVersion);
            matchAccountWithoutIdBuilder.matchColumnSelection(predefinedColumnSelection,
                    selectionVersion);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            matchDataCloudWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            matchAccountWithIdBuilder.dataCloudVersion(dataCloudVersion);
            matchAccountWithoutIdBuilder.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchClientDocument(MatchClientDocument matchClientDocument) {
            matchAccountWithIdBuilder.matchClientDocument(matchClientDocument);
            matchAccountWithoutIdBuilder.matchClientDocument(matchClientDocument);
            matchDataCloudWorkflowBuilder.matchClientDocument(matchClientDocument);
            return this;
        }

        public Builder matchType(MatchCommandType matchCommandType) {
            matchAccountWithIdBuilder.matchType(matchCommandType);
            matchAccountWithoutIdBuilder.matchType(matchCommandType);
            matchDataCloudWorkflowBuilder.matchType(matchCommandType);
            return this;
        }

        public Builder setRetainLatticeAccountId(boolean retainLatticeAccountId) {
            matchAccountWithIdBuilder.setRetainLatticeAccountId(retainLatticeAccountId);
            matchAccountWithoutIdBuilder.setRetainLatticeAccountId(retainLatticeAccountId);
            matchDataCloudWorkflowBuilder.setRetainLatticeAccountId(retainLatticeAccountId);
            return this;
        }

        public Builder matchDestTables(String destTables) {
            matchDataCloudWorkflowBuilder.matchDestTables(destTables);
            matchAccountWithIdBuilder.matchDestTables(destTables);
            matchAccountWithoutIdBuilder.matchDestTables(destTables);
            return this;
        }

        public Builder excludePublicDomains(boolean excludePublicDomains) {
            matchAccountWithIdBuilder.excludePublicDomains(excludePublicDomains);
            matchAccountWithoutIdBuilder.excludePublicDomains(excludePublicDomains);
            matchDataCloudWorkflowBuilder.excludePublicDomains(excludePublicDomains);
            return this;
        }

        public Builder excludeDataCloudAttrs(boolean exclude) {
            matchAccountWithIdBuilder.excludeDataCloudAttrs(exclude);
            matchAccountWithoutIdBuilder.excludeDataCloudAttrs(exclude);
            matchDataCloudWorkflowBuilder.excludeDataCloudAttrs(exclude);
            return this;
        }

        public Builder keepMatchLid(boolean keepLid) {
            matchAccountWithoutIdBuilder.keepMatchLid(keepLid);
            return this;
        }

        public Builder skipDedupStep(boolean skipDedupStep) {
            matchAccountWithIdBuilder.skipDedupStep(skipDedupStep);
            matchAccountWithoutIdBuilder.skipDedupStep(skipDedupStep);
            matchDataCloudWorkflowBuilder.skipDedupStep(skipDedupStep);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            matchDataCloudWorkflowBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            matchAccountWithIdBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            matchAccountWithoutIdBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public Builder matchGroupId(String matchGroupId) {
            matchDataCloudWorkflowBuilder.matchGroupId(matchGroupId);
            matchAccountWithIdBuilder.matchGroupId(matchGroupId);
            matchAccountWithoutIdBuilder.matchGroupId(matchGroupId);
            return this;
        }

        public Builder mapToLatticeAccount(boolean mapToLatticeAccount) {
            matchCdlAccount.setMapToLatticeAccount(mapToLatticeAccount);
            return this;
        }

        public CustomEventMatchWorkflowConfiguration build() {
            matchAccountWithIdBuilder.matchType(MatchStepConfiguration.LDC);
            matchAccountWithoutIdBuilder.matchType(MatchStepConfiguration.LDC);
            matchDataCloudWorkflowBuilder.matchType(MatchStepConfiguration.LDC);
            matchDataCloudWorkflowBuilder.joinWithInternalId(true);

            configuration.setContainerConfiguration("customEventMatchWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(matchDataCloudWorkflowBuilder.build());
            configuration.add(matchAccountWithIdBuilder.build("matchCdlWithAccountIdWorkflow"));
            configuration
                    .add(matchAccountWithoutIdBuilder.build("matchCdlWithoutAccountIdWorkflow"));
            configuration.add(matchCdlAccount);
            configuration.add(matchCdlStep);
            configuration.add(split);
            configuration.add(merge);

            return configuration;
        }
    }
}
