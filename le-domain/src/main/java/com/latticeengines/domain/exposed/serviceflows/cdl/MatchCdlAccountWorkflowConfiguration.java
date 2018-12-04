package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlAccountConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.MatchDataCloudWorkflowConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class MatchCdlAccountWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Scoring.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {
        MatchCdlAccountConfiguration matchCdlAccount = new MatchCdlAccountConfiguration();
        MatchCdlStepConfiguration matchCdlStep = new MatchCdlStepConfiguration();
        MatchDataCloudWorkflowConfiguration.Builder ldcConfigurationBuilder = new MatchDataCloudWorkflowConfiguration.Builder();
        private MatchCdlAccountWorkflowConfiguration configuration = new MatchCdlAccountWorkflowConfiguration();

        public MatchCdlAccountWorkflowConfiguration build() {
            return build("matchCdlAccountWorkflow");
        }

        public MatchCdlAccountWorkflowConfiguration build(String workflowName) {
            configuration.setContainerConfiguration(workflowName, configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(matchCdlAccount);
            configuration.add(matchCdlStep);
            configuration.add(ldcConfigurationBuilder.build());
            return configuration;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            matchCdlAccount.setCustomerSpace(customerSpace);
            matchCdlStep.setCustomerSpace(customerSpace);
            ldcConfigurationBuilder.customer(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            matchCdlAccount.setMicroServiceHostPort(microServiceHostPort);
            matchCdlStep.setMicroServiceHostPort(microServiceHostPort);
            ldcConfigurationBuilder.microServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            matchCdlAccount.setInternalResourceHostPort(internalResourceHostPort);
            matchCdlStep.setInternalResourceHostPort(internalResourceHostPort);
            ldcConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder matchInputTableName(String tableName) {
            matchCdlAccount.setMatchInputTableName(tableName);
            matchCdlStep.setMatchInputTableName(tableName);
            ldcConfigurationBuilder.matchInputTableName(tableName);
            return this;
        }

        public Builder matchCdlTargetTableName(String cdlTargetTableName) {
            matchCdlAccount.setTargetTableName(cdlTargetTableName);
            ldcConfigurationBuilder.matchInputTableName(cdlTargetTableName);
            return this;
        }

        public Builder matchAccountIdColumn(String matchAccountIdColumn) {
            matchCdlAccount.setMatchAccountIdColumn(matchAccountIdColumn);
            return this;
        }

        public Builder matchRequestSource(MatchRequestSource matchRequestSource) {
            ldcConfigurationBuilder.matchRequestSource(matchRequestSource);
            return this;
        }

        public Builder matchQueue(String queue) {
            ldcConfigurationBuilder.matchQueue(queue);
            return this;
        }

        public Builder fetchOnly(boolean fetchOnly) {
            ldcConfigurationBuilder.fetchOnly(fetchOnly);
            return this;
        }

        public Builder matchColumnSelection(Predefined predefinedColumnSelection,
                String selectionVersion) {
            ldcConfigurationBuilder.matchColumnSelection(predefinedColumnSelection,
                    selectionVersion);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            ldcConfigurationBuilder.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchClientDocument(MatchClientDocument matchClientDocument) {
            ldcConfigurationBuilder.matchClientDocument(matchClientDocument);
            return this;
        }

        public Builder matchType(MatchCommandType matchCommandType) {
            ldcConfigurationBuilder.matchType(matchCommandType);
            return this;
        }

        public Builder setRetainLatticeAccountId(boolean retainLatticeAccountId) {
            ldcConfigurationBuilder.setRetainLatticeAccountId(retainLatticeAccountId);
            return this;
        }

        public Builder matchDestTables(String destTables) {
            ldcConfigurationBuilder.matchDestTables(destTables);
            return this;
        }

        public Builder excludePublicDomains(boolean excludePublicDomains) {
            ldcConfigurationBuilder.excludePublicDomains(excludePublicDomains);
            return this;
        }

        public Builder excludeDataCloudAttrs(boolean exclude) {
            ldcConfigurationBuilder.excludeDataCloudAttrs(exclude);
            return this;
        }

        public Builder keepMatchLid(boolean keepLid) {
            ldcConfigurationBuilder.keepMatchLid(keepLid);
            return this;
        }

        public Builder skipDedupStep(boolean skipDedupStep) {
            ldcConfigurationBuilder.skipDedupStep(skipDedupStep);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            ldcConfigurationBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public Builder matchGroupId(String matchGroupId) {
            ldcConfigurationBuilder.matchGroupId(matchGroupId);
            ldcConfigurationBuilder.joinWithInternalId(true);
            return this;
        }

        public Builder matchType(String matchType) {
            ldcConfigurationBuilder.matchType(matchType);
            return this;
        }

    }
}
