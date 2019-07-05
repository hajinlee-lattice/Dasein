package com.latticeengines.domain.exposed.serviceflows.datacloud;

import java.util.Collection;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.MatchJoinType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MatchStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.PrepareMatchDataConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ProcessMatchResultConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.BulkMatchWorkflowConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class MatchDataCloudWorkflowConfiguration extends BaseDataCloudWorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.DataCloud.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {
        PrepareMatchDataConfiguration prepareMatchDataConfiguration = new PrepareMatchDataConfiguration();
        private MatchDataCloudWorkflowConfiguration configuration = new MatchDataCloudWorkflowConfiguration();
        private MatchStepConfiguration match = new MatchStepConfiguration();
        private BulkMatchWorkflowConfiguration.Builder bulkMatchWorkflowConfigurationBuilder = new BulkMatchWorkflowConfiguration.Builder();
        private ProcessMatchResultConfiguration matchResult = new ProcessMatchResultConfiguration();

        public MatchDataCloudWorkflowConfiguration build() {
            configuration.setContainerConfiguration("matchDataCloudWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(match);
            configuration.add(prepareMatchDataConfiguration);
            configuration.add(bulkMatchWorkflowConfigurationBuilder.build());
            configuration.add(matchResult);
            return configuration;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            match.setCustomerSpace(customerSpace);
            prepareMatchDataConfiguration.setCustomerSpace(customerSpace);
            bulkMatchWorkflowConfigurationBuilder.customer(customerSpace);
            matchResult.setCustomer(customerSpace.toString());
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            match.setMicroServiceHostPort(microServiceHostPort);
            prepareMatchDataConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            match.setInternalResourceHostPort(internalResourceHostPort);
            prepareMatchDataConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            matchResult.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder matchInputTableName(String tableName) {
            match.setInputTableName(tableName);
            prepareMatchDataConfiguration.setInputTableName(tableName);
            return this;
        }

        public Builder matchRequestSource(MatchRequestSource matchRequestSource) {
            match.setMatchRequestSource(matchRequestSource);
            return this;
        }

        public Builder matchQueue(String queue) {
            match.setMatchQueue(queue);
            return this;
        }

        public Builder matchColumnSelection(Predefined predefinedColumnSelection,
                String selectionVersion) {
            match.setPredefinedColumnSelection(predefinedColumnSelection);
            match.setPredefinedSelectionVersion(selectionVersion);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            match.setDataCloudVersion(dataCloudVersion);
            matchResult.setDataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchClientDocument(MatchClientDocument matchClientDocument) {
            match.setDbUrl(matchClientDocument.getUrl());
            match.setDbUser(matchClientDocument.getUsername());
            match.setDbPasswordEncrypted(matchClientDocument.getEncryptedPassword());
            match.setMatchClient(matchClientDocument.getMatchClient().name());
            return this;
        }

        public Builder matchType(MatchCommandType matchCommandType) {
            match.setMatchCommandType(matchCommandType);
            return this;
        }

        public Builder setRetainLatticeAccountId(boolean retainLatticeAccountId) {
            match.setRetainLatticeAccountId(retainLatticeAccountId);
            return this;
        }

        public Builder matchDestTables(String destTables) {
            match.setDestTables(destTables);
            return this;
        }

        public Builder excludePublicDomains(boolean excludePublicDomains) {
            match.setExcludePublicDomain(excludePublicDomains);
            return this;
        }

        public Builder excludeDataCloudAttrs(boolean exclude) {
            matchResult.setExcludeDataCloudAttrs(exclude);
            return this;
        }

        public Builder keepMatchLid(boolean keepLid) {
            matchResult.setKeepLid(keepLid);
            return this;
        }

        public Builder skipDedupStep(boolean skipDedupStep) {
            match.setSkipDedupe(skipDedupStep);
            return this;
        }

        public Builder fetchOnly(boolean fetchOnly) {
            match.setFetchOnly(fetchOnly);
            return this;
        }

        public Builder skipMatchingStep(boolean skipMatchingStep) {
            match.setSkipStep(skipMatchingStep);
            prepareMatchDataConfiguration.setSkipStep(skipMatchingStep);
            matchResult.setSkipStep(skipMatchingStep);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            match.setSourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public Builder matchJoinType(MatchJoinType matchJoinType) {
            match.setMatchJoinType(matchJoinType);
            return this;
        }

        public Builder treatPublicDomainAsNormalDomain(boolean publicDomainAsNormalDomain) {
            match.setPublicDomainAsNormalDomain(publicDomainAsNormalDomain);
            return this;
        }

        public Builder matchHdfsPod(String matchHdfsPod) {
            match.setMatchHdfsPod(matchHdfsPod);
            return this;
        }

        public Builder matchType(String matchType) {
            match.setMatchType(matchType);
            return this;
        }

        public Builder matchGroupId(String matchGroupId) {
            prepareMatchDataConfiguration.setMatchGroupId(matchGroupId);
            matchResult.setMatchGroupId(matchGroupId);
            return this;
        }

        public Builder joinWithInternalId(boolean joinWithInternalId) {
            matchResult.setJoinInternalId(joinWithInternalId);
            return this;
        }

    }
}
