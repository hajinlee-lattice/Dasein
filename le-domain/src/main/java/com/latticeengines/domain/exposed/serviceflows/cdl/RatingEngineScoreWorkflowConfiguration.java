package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.MatchJoinType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlEventTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlTargetTableFilterConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MatchStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ProcessMatchResultConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ScoreStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.CombineInputTableWithScoreDataFlowConfiguration;

public class RatingEngineScoreWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    private RatingEngineScoreWorkflowConfiguration() {
    }

    public static class Builder {

        private RatingEngineScoreWorkflowConfiguration configuration = new RatingEngineScoreWorkflowConfiguration();
        private MicroserviceStepConfiguration microserviceStepConfiguration = new MicroserviceStepConfiguration();
        private MatchStepConfiguration match = new MatchStepConfiguration();
        private ScoreStepConfiguration score = new ScoreStepConfiguration();
        private CombineInputTableWithScoreDataFlowConfiguration combineInputWithScores = new CombineInputTableWithScoreDataFlowConfiguration();
        private ExportStepConfiguration export = new ExportStepConfiguration();
        private ProcessMatchResultConfiguration matchResult = new ProcessMatchResultConfiguration();

        private CreateCdlTargetTableFilterConfiguration cdlTargetTableTupleFilter = new CreateCdlTargetTableFilterConfiguration();
        private CreateCdlEventTableConfiguration cdlEventTable = new CreateCdlEventTableConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("ratingEngineScoreWorkflow", customerSpace,
                    "ratingEngineScoreWorkflow");
            microserviceStepConfiguration.setCustomerSpace(customerSpace);
            match.setCustomerSpace(customerSpace);
            score.setCustomerSpace(customerSpace);
            combineInputWithScores.setCustomerSpace(customerSpace);
            export.setCustomerSpace(customerSpace);
            matchResult.setCustomerSpace(customerSpace);
            cdlTargetTableTupleFilter.setCustomerSpace(customerSpace);
            cdlEventTable.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            microserviceStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            matchResult.setMicroServiceHostPort(microServiceHostPort);
            cdlTargetTableTupleFilter.setMicroServiceHostPort(microServiceHostPort);
            cdlEventTable.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            match.setInternalResourceHostPort(internalResourceHostPort);
            score.setInternalResourceHostPort(internalResourceHostPort);
            combineInputWithScores.setInternalResourceHostPort(internalResourceHostPort);
            export.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            cdlTargetTableTupleFilter.setInternalResourceHostPort(internalResourceHostPort);
            cdlEventTable.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder inputTableName(String tableName) {
            match.setInputTableName(tableName);
            cdlEventTable.setOutputTableName(tableName);
            combineInputWithScores.setDataFlowParams(new CombineInputTableWithScoreParameters(null, tableName));
            return this;
        }

        public Builder filterTableName(String targetFilterTableName) {
            cdlTargetTableTupleFilter.setTargetFilterTableName(targetFilterTableName);
            return this;
        }

        public Builder filterQuery(EventFrontEndQuery targetQuery) {
            cdlTargetTableTupleFilter.setTargetQuery(targetQuery);
            return this;
        }

        public Builder modelId(String modelId) {
            score.setModelId(modelId);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            match.setSourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public Builder excludeDataCloudAttrs(boolean exclude) {
            matchResult.setExcludeDataCloudAttrs(exclude);
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

        public Builder matchDestTables(String destTables) {
            match.setDestTables(destTables);
            return this;
        }

        public Builder outputFilename(String outputFilename) {
            export.setExportDestination(ExportDestination.FILE);
            export.putProperty(ExportProperty.TARGET_FILE_NAME, outputFilename);
            return this;
        }

        public Builder outputFileFormat(ExportFormat format) {
            export.setExportFormat(format);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public Builder matchJoinType(MatchJoinType matchJoinType) {
            match.setMatchJoinType(matchJoinType);
            return this;
        }

        public Builder columnSelection(ColumnSelection customizedColumnSelection) {
            match.setCustomizedColumnSelection(customizedColumnSelection);
            return this;
        }

        public Builder columnSelection(Predefined predefinedColumnSelection, String selectionVersion) {
            match.setPredefinedColumnSelection(predefinedColumnSelection);
            match.setPredefinedSelectionVersion(selectionVersion);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            match.setDataCloudVersion(dataCloudVersion);
            matchResult.setDataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchRequestSource(MatchRequestSource matchRequestSource) {
            match.setMatchRequestSource(matchRequestSource);
            return this;
        }

        public Builder bucketMetadata(List<BucketMetadata> bucketMetadataList) {
            combineInputWithScores.setBucketMetadata(bucketMetadataList);
            return this;
        }

        public Builder matchQueue(String queue) {
            match.setMatchQueue(queue);
            return this;
        }

        public Builder setUniqueKeyColumn(String uniqueKeyColumn) {
            score.setUniqueKeyColumn(uniqueKeyColumn);
            return this;
        }

        public Builder setUseScorederivation(boolean useScorederivation) {
            score.setUseScorederivation(useScorederivation);
            return this;
        }

        public Builder setEventColumn(String eventColumn) {
            cdlEventTable.setEventColumn(eventColumn);
            return this;
        }

        public RatingEngineScoreWorkflowConfiguration build() {
            match.microserviceStepConfiguration(microserviceStepConfiguration);
            score.microserviceStepConfiguration(microserviceStepConfiguration);
            combineInputWithScores.microserviceStepConfiguration(microserviceStepConfiguration);
            export.microserviceStepConfiguration(microserviceStepConfiguration);
            cdlTargetTableTupleFilter.microserviceStepConfiguration(microserviceStepConfiguration);
            cdlEventTable.microserviceStepConfiguration(microserviceStepConfiguration);

            configuration.add(cdlTargetTableTupleFilter);
            configuration.add(cdlEventTable);
            configuration.add(match);
            configuration.add(matchResult);
            configuration.add(score);
            configuration.add(combineInputWithScores);
            configuration.add(export);

            return configuration;
        }

    }
}
