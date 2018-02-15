package com.latticeengines.domain.exposed.serviceflows.leadprioritization;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.MatchJoinType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.core.steps.BaseReportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.ScoreWorkflowConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class ImportMatchAndScoreWorkflowConfiguration extends BaseLPWorkflowConfiguration {

    private ImportMatchAndScoreWorkflowConfiguration() {
    }

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Scoring.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {
        private ImportMatchAndScoreWorkflowConfiguration configuration = new ImportMatchAndScoreWorkflowConfiguration();

        private ImportStepConfiguration importDataConfiguration = new ImportStepConfiguration();

        private BaseReportStepConfiguration registerReport = new BaseReportStepConfiguration();

        private ScoreWorkflowConfiguration.Builder scoreWorkflowConfigurationBuilder = new ScoreWorkflowConfiguration.Builder();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("importMatchAndScoreWorkflow", customerSpace,
                    "importMatchAndScoreWorkflow");
            importDataConfiguration.setCustomerSpace(customerSpace);
            registerReport.setCustomerSpace(customerSpace);
            scoreWorkflowConfigurationBuilder.customer(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            importDataConfiguration.setMicroServiceHostPort(microServiceHostPort);
            registerReport.setMicroServiceHostPort(microServiceHostPort);
            scoreWorkflowConfigurationBuilder.microServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder sourceFileName(String sourceFileName) {
            importDataConfiguration.setSourceFileName(sourceFileName);
            return this;
        }

        public Builder sourceType(SourceType sourceType) {
            importDataConfiguration.setSourceType(sourceType);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            importDataConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            registerReport.setInternalResourceHostPort(internalResourceHostPort);
            scoreWorkflowConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder reportNamePrefix(String reportName) {
            registerReport.setReportNamePrefix(reportName);
            return this;
        }

        public Builder inputTableName(String tableName) {
            scoreWorkflowConfigurationBuilder.inputTableName(tableName);
            return this;
        }

        public Builder modelId(String modelId) {
            scoreWorkflowConfigurationBuilder.modelId(modelId);
            return this;
        }

        public Builder excludeDataCloudAttrs(boolean exclude) {
            scoreWorkflowConfigurationBuilder.excludeDataCloudAttrs(exclude);
            return this;
        }

        public Builder matchClientDocument(MatchClientDocument matchClientDocument) {
            scoreWorkflowConfigurationBuilder.matchClientDocument(matchClientDocument);
            return this;
        }

        public Builder matchType(MatchCommandType matchCommandType) {
            scoreWorkflowConfigurationBuilder.matchType(matchCommandType);
            return this;
        }

        public Builder matchDestTables(String destTables) {
            scoreWorkflowConfigurationBuilder.matchDestTables(destTables);
            return this;
        }

        public Builder matchColumnSelection(ColumnSelection customizedColumnSelection) {
            scoreWorkflowConfigurationBuilder.columnSelection(customizedColumnSelection);
            return this;
        }

        public Builder matchColumnSelection(Predefined predefinedColumnSelection, String selectionVersion) {
            scoreWorkflowConfigurationBuilder.columnSelection(predefinedColumnSelection, selectionVersion);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            scoreWorkflowConfigurationBuilder.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchDebugEnabled(boolean matchDebugEnabled) {
            scoreWorkflowConfigurationBuilder.matchDebugEnabled(matchDebugEnabled);
            return this;
        }

        public Builder matchRequestSource(MatchRequestSource matchRequestSource) {
            scoreWorkflowConfigurationBuilder.matchRequestSource(matchRequestSource);
            return this;
        }

        public Builder outputFilename(String outputFilename) {
            scoreWorkflowConfigurationBuilder.outputFilename(outputFilename);
            return this;
        }

        public Builder outputFileFormat(ExportFormat format) {
            scoreWorkflowConfigurationBuilder.outputFileFormat(format);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public Builder internalResourcePort(String internalResourceHostPort) {
            scoreWorkflowConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder matchJoinType(MatchJoinType matchJoinType) {
            scoreWorkflowConfigurationBuilder.matchJoinType(matchJoinType);
            return this;
        }

        public Builder transformationGroup(TransformationGroup transformationGroup) {
            scoreWorkflowConfigurationBuilder.transformationGroup(transformationGroup);
            return this;
        }

        public Builder transformDefinitions(List<TransformDefinition> transforms) {
            scoreWorkflowConfigurationBuilder.transformDefinitions(transforms);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            scoreWorkflowConfigurationBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public Builder bucketMetadata(List<BucketMetadata> bucketMetadata) {
            scoreWorkflowConfigurationBuilder.bucketMetadata(bucketMetadata);
            return this;
        }

        public ImportMatchAndScoreWorkflowConfiguration build() {
            configuration.add(importDataConfiguration);
            configuration.add(registerReport);
            configuration.add(scoreWorkflowConfigurationBuilder.build());
            return configuration;
        }

    }
}
