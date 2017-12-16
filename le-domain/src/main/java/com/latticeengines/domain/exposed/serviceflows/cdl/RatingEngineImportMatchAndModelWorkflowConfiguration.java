package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlEventTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlEventTableFilterConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlTagetTableFilterConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.SetCdlConfigurationForScoringConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MatchStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ModelStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ProcessMatchResultConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ScoreStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.CombineInputTableWithScoreDataFlowConfiguration;

public class RatingEngineImportMatchAndModelWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private RatingEngineImportMatchAndModelWorkflowConfiguration configuration = new RatingEngineImportMatchAndModelWorkflowConfiguration();
        private ModelStepConfiguration model = new ModelStepConfiguration();
        private MatchStepConfiguration match = new MatchStepConfiguration();
        private CreateCdlEventTableConfiguration cdlEventTable = new CreateCdlEventTableConfiguration();
        private CreateCdlEventTableFilterConfiguration cdlEventTableTupleFilter = new CreateCdlEventTableFilterConfiguration();
        private ExportStepConfiguration export = new ExportStepConfiguration();
        private ProcessMatchResultConfiguration matchResult = new ProcessMatchResultConfiguration();

        private SetCdlConfigurationForScoringConfiguration setConfigForScoring = new SetCdlConfigurationForScoringConfiguration();
        private CreateCdlTagetTableFilterConfiguration cdlScoingTableTupleFilter = new CreateCdlTagetTableFilterConfiguration();
        private ScoreStepConfiguration score = new ScoreStepConfiguration();
        private CombineInputTableWithScoreDataFlowConfiguration combineInputWithScores = new CombineInputTableWithScoreDataFlowConfiguration();

        public Builder microServiceHostPort(String microServiceHostPort) {
            model.setMicroServiceHostPort(microServiceHostPort);
            match.setMicroServiceHostPort(microServiceHostPort);
            cdlEventTable.setMicroServiceHostPort(microServiceHostPort);
            cdlEventTableTupleFilter.setMicroServiceHostPort(microServiceHostPort);
            export.setMicroServiceHostPort(microServiceHostPort);
            matchResult.setMicroServiceHostPort(microServiceHostPort);
            setConfigForScoring.setMicroServiceHostPort(microServiceHostPort);
            cdlScoingTableTupleFilter.setMicroServiceHostPort(microServiceHostPort);
            score.setMicroServiceHostPort(microServiceHostPort);
            combineInputWithScores.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("ratingEngineImportMatchAndModelWorkflow", customerSpace,
                    "ratingEngineImportMatchAndModelWorkflow");
            model.setCustomerSpace(customerSpace);
            match.setCustomerSpace(customerSpace);
            cdlEventTable.setCustomerSpace(customerSpace);
            cdlEventTableTupleFilter.setCustomerSpace(customerSpace);
            export.setCustomerSpace(customerSpace);
            matchResult.setCustomerSpace(customerSpace);
            setConfigForScoring.setCustomerSpace(customerSpace);
            cdlScoingTableTupleFilter.setCustomerSpace(customerSpace);
            score.setCustomerSpace(customerSpace);
            combineInputWithScores.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder matchInputTableName(String tableName) {
            match.setInputTableName(tableName);
            cdlEventTable.setOutputTableName(tableName);
            return this;
        }

        public Builder filterTableNames(String trainFilterTableName, String eventFilterTableName,
                String targetFilterTableName) {
            cdlEventTableTupleFilter.setTrainFilterTableName(trainFilterTableName);
            cdlEventTableTupleFilter.setEventFilterTableName(eventFilterTableName);
            cdlScoingTableTupleFilter.setTargetFilterTableName(targetFilterTableName);
            return this;
        }

        public Builder filterQueries(FrontEndQuery trainQuery, FrontEndQuery eventQuery, FrontEndQuery targetQuery) {
            cdlEventTableTupleFilter.setTrainQuery(trainQuery);
            cdlEventTableTupleFilter.setEventQuery(eventQuery);
            cdlScoingTableTupleFilter.setTargetQuery(targetQuery);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            model.setInternalResourceHostPort(internalResourceHostPort);
            match.setInternalResourceHostPort(internalResourceHostPort);
            cdlEventTable.setInternalResourceHostPort(internalResourceHostPort);
            cdlEventTableTupleFilter.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            setConfigForScoring.setInternalResourceHostPort(internalResourceHostPort);
            cdlScoingTableTupleFilter.setInternalResourceHostPort(internalResourceHostPort);
            score.setInternalResourceHostPort(internalResourceHostPort);
            combineInputWithScores.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder userId(String userId) {
            model.setUserName(userId);
            return this;
        }

        public Builder modelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            model.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            setConfigForScoring.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            return this;
        }

        public Builder excludePublicDomains(boolean excludePublicDomains) {
            match.setExcludePublicDomain(excludePublicDomains);
            model.addProvenanceProperty(ProvenancePropertyName.ExcludePublicDomains, excludePublicDomains);
            return this;
        }

        public Builder setRetainLatticeAccountId(boolean retainLatticeAccountId) {
            match.setRetainLatticeAccountId(retainLatticeAccountId);
            return this;
        }

        public Builder excludeDataCloudAttrs(boolean exclude) {
            matchResult.setExcludeDataCloudAttrs(exclude);
            model.addProvenanceProperty(ProvenancePropertyName.ExcludePropdataColumns, exclude);
            return this;
        }

        public Builder skipDedupStep(boolean skipDedupStep) {
            match.setSkipDedupe(skipDedupStep);
            matchResult.setSkipDedupe(skipDedupStep);
            model.addProvenanceProperty(ProvenancePropertyName.IsOneLeadPerDomain, !skipDedupStep);
            return this;
        }

        public Builder matchRequestSource(MatchRequestSource matchRequestSource) {
            match.setMatchRequestSource(matchRequestSource);
            return this;
        }

        public Builder matchColumnSelection(ColumnSelection customizedColumnSelection) {
            match.setCustomizedColumnSelection(customizedColumnSelection);
            return this;
        }

        public Builder matchColumnSelection(Predefined predefinedColumnSelection, String selectionVersion) {
            match.setPredefinedColumnSelection(predefinedColumnSelection);
            match.setPredefinedSelectionVersion(selectionVersion);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            match.setDataCloudVersion(dataCloudVersion);
            matchResult.setDataCloudVersion(dataCloudVersion);
            model.setDataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            model.setSourceSchemaInterpretation(sourceSchemaInterpretation);
            cdlEventTable.setSourceSchemaInterpretation(sourceSchemaInterpretation);
            match.setSourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public Builder trainingTableName(String trainingTableName) {
            model.setTrainingTableName(trainingTableName);
            combineInputWithScores.setDataFlowParams(new CombineInputTableWithScoreParameters(null, trainingTableName));
            return this;
        }

        public Builder modelName(String modelName) {
            model.setModelName(modelName);
            return this;
        }

        public Builder displayName(String displayName) {
            model.setDisplayName(displayName);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            setConfigForScoring.setInputProperties(inputProperties);
            return this;
        }

        public Builder dataRules(List<DataRule> dataRules) {
            model.setDataRules(dataRules);
            return this;
        }

        public Builder isDefaultDataRules(boolean isDefaultDataRules) {
            model.setDefaultDataRuleConfiguration(isDefaultDataRules);
            return this;
        }

        public Builder addProvenanceProperty(ProvenancePropertyName propertyName, Object value) {
            model.addProvenanceProperty(propertyName, value);
            return this;
        }

        public Builder pivotArtifactPath(String pivotArtifactPath) {
            model.setPivotArtifactPath(pivotArtifactPath);
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

        public Builder moduleName(String moduleName) {
            model.setModuleName(moduleName);
            return this;
        }

        public Builder enableV2Profiling(boolean v2ProfilingEnabled) {
            model.setV2ProfilingEnabled(v2ProfilingEnabled);
            model.addProvenanceProperty(ProvenancePropertyName.IsV2ProfilingEnabled, v2ProfilingEnabled);
            return this;
        }

        public Builder cdlModel(boolean isCdlModel) {
            model.setCdlModel(isCdlModel);
            return this;
        }

        public Builder notesContent(String notesContent) {
            model.setNotesContent(notesContent);
            return this;
        }

        public Builder matchQueue(String queue) {
            match.setMatchQueue(queue);
            return this;
        }

        public Builder setActivateModelSummaryByDefault(boolean value) {
            model.setActivateModelSummaryByDefault(value);
            return this;
        }

        public Builder setUniqueKeyColumn(String uniqueKeyColumn) {
            score.setUniqueKeyColumn(uniqueKeyColumn);
            return this;
        }

        public RatingEngineImportMatchAndModelWorkflowConfiguration build() {
            export.setUsingDisplayName(Boolean.FALSE);
            export.setExportDestination(ExportDestination.FILE);
            export.setExportFormat(ExportFormat.CSV);

            configuration.add(cdlEventTableTupleFilter);
            configuration.add(cdlEventTable);
            configuration.add(match);
            configuration.add(model);
            configuration.add(matchResult);
            configuration.add(export);
            configuration.add(setConfigForScoring);
            configuration.add(cdlScoingTableTupleFilter);
            configuration.add(score);
            configuration.add(combineInputWithScores);

            return configuration;
        }

    }
}
