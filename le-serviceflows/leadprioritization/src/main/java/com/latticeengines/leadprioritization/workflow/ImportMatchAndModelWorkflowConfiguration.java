package com.latticeengines.leadprioritization.workflow;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.dataflow.flows.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.transform.TransformationPipeline;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.AddStandardAttributesConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.CombineInputTableWithScoreDataFlowConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.CreatePrematchEventTableReportConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.DedupEventTableConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.PivotScoreAndEventConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.SetConfigurationForScoringConfiguration;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.export.ExportStepConfiguration;
import com.latticeengines.serviceflows.workflow.importdata.ImportStepConfiguration;
import com.latticeengines.serviceflows.workflow.match.MatchStepConfiguration;
import com.latticeengines.serviceflows.workflow.match.ProcessMatchResultConfiguration;
import com.latticeengines.serviceflows.workflow.modeling.ModelStepConfiguration;
import com.latticeengines.serviceflows.workflow.report.BaseReportStepConfiguration;
import com.latticeengines.serviceflows.workflow.scoring.ScoreStepConfiguration;

public class ImportMatchAndModelWorkflowConfiguration extends WorkflowConfiguration {
    public static class Builder {
        private ImportMatchAndModelWorkflowConfiguration configuration = new ImportMatchAndModelWorkflowConfiguration();
        private ImportStepConfiguration importData = new ImportStepConfiguration();
        private BaseReportStepConfiguration registerReport = new BaseReportStepConfiguration();
        private CreatePrematchEventTableReportConfiguration createEventTableReport = new CreatePrematchEventTableReportConfiguration();
        private DedupEventTableConfiguration dedupEventTable = new DedupEventTableConfiguration();
        private ModelStepConfiguration model = new ModelStepConfiguration();
        private MatchStepConfiguration match = new MatchStepConfiguration();
        private AddStandardAttributesConfiguration addStandardAttributes = new AddStandardAttributesConfiguration();
        private ExportStepConfiguration export = new ExportStepConfiguration();
        private ProcessMatchResultConfiguration matchResult = new ProcessMatchResultConfiguration();
        private SetConfigurationForScoringConfiguration setConfigForScoring = new SetConfigurationForScoringConfiguration();
        private ScoreStepConfiguration score = new ScoreStepConfiguration();
        private CombineInputTableWithScoreDataFlowConfiguration combineInputWithScores = new CombineInputTableWithScoreDataFlowConfiguration();
        private PivotScoreAndEventConfiguration pivotScoreAndEvent = new PivotScoreAndEventConfiguration();

        public Builder microServiceHostPort(String microServiceHostPort) {
            importData.setMicroServiceHostPort(microServiceHostPort);
            registerReport.setMicroServiceHostPort(microServiceHostPort);
            createEventTableReport.setMicroServiceHostPort(microServiceHostPort);
            dedupEventTable.setMicroServiceHostPort(microServiceHostPort);
            model.setMicroServiceHostPort(microServiceHostPort);
            match.setMicroServiceHostPort(microServiceHostPort);
            addStandardAttributes.setMicroServiceHostPort(microServiceHostPort);
            export.setMicroServiceHostPort(microServiceHostPort);
            matchResult.setMicroServiceHostPort(microServiceHostPort);
            setConfigForScoring.setMicroServiceHostPort(microServiceHostPort);
            score.setMicroServiceHostPort(microServiceHostPort);
            combineInputWithScores.setMicroServiceHostPort(microServiceHostPort);
            pivotScoreAndEvent.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("importMatchAndModelWorkflow", customerSpace,
                    "ImportMatchAndModelWorkflow");
            importData.setCustomerSpace(customerSpace);
            registerReport.setCustomerSpace(customerSpace);
            createEventTableReport.setCustomerSpace(customerSpace);
            dedupEventTable.setCustomerSpace(customerSpace);
            model.setCustomerSpace(customerSpace);
            match.setCustomerSpace(customerSpace);
            addStandardAttributes.setCustomerSpace(customerSpace);
            export.setCustomerSpace(customerSpace);
            matchResult.setCustomerSpace(customerSpace);
            setConfigForScoring.setCustomerSpace(customerSpace);
            score.setCustomerSpace(customerSpace);
            combineInputWithScores.setCustomerSpace(customerSpace);
            pivotScoreAndEvent.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder sourceFileName(String sourceFileName) {
            importData.setSourceFileName(sourceFileName);
            return this;
        }

        public Builder eventTableReportSourceFileName(String eventTableReportSourceFileName) {
            createEventTableReport.setSourceTableName(eventTableReportSourceFileName);
            return this;
        }

        public Builder dedupTargetTableName(String targetTableName) {
            dedupEventTable.setTargetTableName(targetTableName);
            match.setInputTableName(targetTableName);
            return this;
        }

        public Builder minPositiveEvents(long minPositiveEvents) {
            createEventTableReport.setMinPositiveEvents(minPositiveEvents);
            return this;
        }

        public Builder minNegativeEvents(long minNegativeEvents) {
            createEventTableReport.setMinNegativeEvents(minNegativeEvents);
            return this;
        }

        public Builder minRows(long minRows) {
            createEventTableReport.setMinRows(minRows);
            return this;
        }

        public Builder sourceType(SourceType sourceType) {
            importData.setSourceType(sourceType);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            importData.setInternalResourceHostPort(internalResourceHostPort);
            registerReport.setInternalResourceHostPort(internalResourceHostPort);
            createEventTableReport.setInternalResourceHostPort(internalResourceHostPort);
            dedupEventTable.setInternalResourceHostPort(internalResourceHostPort);
            model.setInternalResourceHostPort(internalResourceHostPort);
            match.setInternalResourceHostPort(internalResourceHostPort);
            addStandardAttributes.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            setConfigForScoring.setInternalResourceHostPort(internalResourceHostPort);
            score.setInternalResourceHostPort(internalResourceHostPort);
            combineInputWithScores.setInternalResourceHostPort(internalResourceHostPort);
            pivotScoreAndEvent.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder importReportNamePrefix(String reportName) {
            registerReport.setReportNamePrefix(reportName);
            return this;
        }

        public Builder eventTableReportNamePrefix(String eventTableReportName) {
            createEventTableReport.setReportNamePrefix(eventTableReportName);
            return this;
        }

        public Builder dedupDataFlowBeanName(String beanName) {
            dedupEventTable.setBeanName(beanName);
            return this;
        }

        public Builder dedupDataFlowParams(DataFlowParameters dataFlowParameters) {
            dedupEventTable.setDataFlowParams(dataFlowParameters);
            return this;
        }

        public Builder dedupFlowExtraSources(Map<String, String> extraSources) {
            dedupEventTable.setExtraSources(extraSources);
            return this;
        }

        public Builder modelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            model.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            setConfigForScoring.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            return this;
        }

        public Builder matchClientDocument(MatchClientDocument matchClientDocument) {
            match.setDbUrl(matchClientDocument.getUrl());
            match.setDbUser(matchClientDocument.getUsername());
            match.setDbPasswordEncrypted(matchClientDocument.getEncryptedPassword());
            match.setMatchClient(matchClientDocument.getMatchClient().name());
            return this;
        }

        public Builder excludePublicDomains(boolean excludePublicDomains) {
            match.setExcludeUnmatchedWithPublicDomain(excludePublicDomains);
            model.addProvenanceProperty(ProvenancePropertyName.ExcludePublicDomains, excludePublicDomains);
            return this;
        }

        public Builder skipMatchingStep(boolean skipMatchingStep) {
            match.setSkipStep(skipMatchingStep);
            matchResult.setSkipStep(skipMatchingStep);
            model.addProvenanceProperty(ProvenancePropertyName.ExcludePropdataColumns, skipMatchingStep);
            return this;
        }

        public Builder skipDedupStep(boolean skipDedupStep) {
            dedupEventTable.setSkipStep(skipDedupStep);
            model.addProvenanceProperty(ProvenancePropertyName.IsOneLeadPerDomain, !skipDedupStep);
            return this;
        }

        public Builder skipStandardTransform(boolean skipTransform) {
            addStandardAttributes.setSkipStep(skipTransform);
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

        /**
         * You can provide a full column selection object or the name of a
         * predefined selection. When both are present, predefined one will be
         * used.
         * 
         * @param customizedColumnSelection
         * @return
         */
        public Builder matchColumnSelection(ColumnSelection customizedColumnSelection) {
            match.setCustomizedColumnSelection(customizedColumnSelection);
            return this;
        }

        /**
         * You can provide a full column selection object or the name of a
         * predefined selection. When both are present, predefined one will be
         * used. If selectionVersion is empty, will use current version.
         * 
         * @param predefinedColumnSelection
         * @return
         */
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
            setConfigForScoring.setInputProperties(inputProperties);
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public Builder transformationGroup(TransformationGroup transformationGroup) {
            addStandardAttributes.setTransformationGroup(transformationGroup);
            addStandardAttributes.setTransforms(TransformationPipeline.getTransforms(transformationGroup));
            model.addProvenanceProperty(ProvenancePropertyName.TransformationGroupName, transformationGroup.getName());
            return this;
        }

        public Builder runTimeParams(Map<String, String> runTimeParams) {
            model.setRunTimeParams(runTimeParams);
            addStandardAttributes.setRuntimeParams(runTimeParams);
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

        public Builder moduleName(String moduleName) {
            model.setModuleName(moduleName);
            return this;
        }

        public Builder enableV2Profiling(boolean v2ProfilingEnabled) {
            model.setV2ProfilingEnabled(v2ProfilingEnabled);
            model.addProvenanceProperty(ProvenancePropertyName.IsV2ProfilingEnabled, v2ProfilingEnabled);
            return this;
        }

        public ImportMatchAndModelWorkflowConfiguration build() {
            export.setUsingDisplayName(Boolean.FALSE);
            export.setExportDestination(ExportDestination.FILE);
            export.setExportFormat(ExportFormat.CSV);
            match.setMatchQueue(LedpQueueAssigner.getModelingQueueNameForSubmission());

            configuration.add(importData);
            configuration.add(registerReport);
            configuration.add(createEventTableReport);
            configuration.add(dedupEventTable);
            configuration.add(match);
            configuration.add(model);
            configuration.add(addStandardAttributes);
            configuration.add(matchResult);
            configuration.add(export);
            configuration.add(setConfigForScoring);
            configuration.add(score);
            configuration.add(combineInputWithScores);
            configuration.add(pivotScoreAndEvent);

            return configuration;
        }

    }
}
