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
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.AddStandardAttributesConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.CombineInputTableWithScoreDataFlowConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.DedupEventTableConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.PivotScoreAndEventConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.SetConfigurationForScoringConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.ResolveMetadataFromUserRefinedAttributesConfiguration;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.export.ExportStepConfiguration;
import com.latticeengines.serviceflows.workflow.match.MatchStepConfiguration;
import com.latticeengines.serviceflows.workflow.match.ProcessMatchResultConfiguration;
import com.latticeengines.serviceflows.workflow.modeling.ModelStepConfiguration;
import com.latticeengines.serviceflows.workflow.scoring.ScoreStepConfiguration;

public class MatchAndModelWorkflowConfiguration extends WorkflowConfiguration {
    public static class Builder {
        private MatchAndModelWorkflowConfiguration configuration = new MatchAndModelWorkflowConfiguration();
        private DedupEventTableConfiguration dedupEventTable = new DedupEventTableConfiguration();
        private MatchStepConfiguration match = new MatchStepConfiguration();
        private ModelStepConfiguration model = new ModelStepConfiguration();
        private ExportStepConfiguration export = new ExportStepConfiguration();
        private AddStandardAttributesConfiguration addStandardAttributes = new AddStandardAttributesConfiguration();
        private ProcessMatchResultConfiguration matchResult = new ProcessMatchResultConfiguration();
        private ResolveMetadataFromUserRefinedAttributesConfiguration resolveAttributes = new ResolveMetadataFromUserRefinedAttributesConfiguration();
        private SetConfigurationForScoringConfiguration setConfigForScoring = new SetConfigurationForScoringConfiguration();
        private ScoreStepConfiguration score = new ScoreStepConfiguration();
        private CombineInputTableWithScoreDataFlowConfiguration combineInputWithScores = new CombineInputTableWithScoreDataFlowConfiguration();
        private PivotScoreAndEventConfiguration pivotScoreAndEvent = new PivotScoreAndEventConfiguration();

        public Builder microServiceHostPort(String microServiceHostPort) {
            dedupEventTable.setMicroServiceHostPort(microServiceHostPort);
            match.setMicroServiceHostPort(microServiceHostPort);
            model.setMicroServiceHostPort(microServiceHostPort);
            export.setMicroServiceHostPort(microServiceHostPort);
            matchResult.setMicroServiceHostPort(microServiceHostPort);
            addStandardAttributes.setMicroServiceHostPort(microServiceHostPort);
            resolveAttributes.setMicroServiceHostPort(microServiceHostPort);
            setConfigForScoring.setMicroServiceHostPort(microServiceHostPort);
            score.setMicroServiceHostPort(microServiceHostPort);
            combineInputWithScores.setMicroServiceHostPort(microServiceHostPort);
            pivotScoreAndEvent.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            dedupEventTable.setInternalResourceHostPort(internalResourceHostPort);
            match.setInternalResourceHostPort(internalResourceHostPort);
            model.setInternalResourceHostPort(internalResourceHostPort);
            export.setInternalResourceHostPort(internalResourceHostPort);
            addStandardAttributes.setInternalResourceHostPort(internalResourceHostPort);
            resolveAttributes.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            setConfigForScoring.setInternalResourceHostPort(internalResourceHostPort);
            score.setInternalResourceHostPort(internalResourceHostPort);
            combineInputWithScores.setInternalResourceHostPort(internalResourceHostPort);
            pivotScoreAndEvent.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            dedupEventTable.setCustomerSpace(customerSpace);
            match.setCustomerSpace(customerSpace);
            model.setCustomerSpace(customerSpace);
            export.setCustomerSpace(customerSpace);
            addStandardAttributes.setCustomerSpace(customerSpace);
            matchResult.setCustomerSpace(customerSpace);
            resolveAttributes.setCustomerSpace(customerSpace);
            setConfigForScoring.setCustomerSpace(customerSpace);
            score.setCustomerSpace(customerSpace);
            combineInputWithScores.setCustomerSpace(customerSpace);
            pivotScoreAndEvent.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder workflow(String workflowName) {
            configuration.setWorkflowName(workflowName);
            configuration.setName(workflowName);
            return this;
        }

        public Builder modelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            model.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            setConfigForScoring.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
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

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            model.setSourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public Builder trainingTableName(String trainingTableName) {
            model.setTrainingTableName(trainingTableName);
            combineInputWithScores.setDataFlowParams(new CombineInputTableWithScoreParameters(null, trainingTableName));
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            setConfigForScoring.setInputProperties(inputProperties);
            return this;
        }

        public Builder transformationGroup(TransformationGroup transformationGroup) {
            addStandardAttributes.setTransformationGroup(transformationGroup);
            model.addProvenanceProperty(ProvenancePropertyName.TransformationGroupName, transformationGroup.getName());
            return this;
        }

        public Builder sourceModelSummary(ModelSummary modelSummary) {
            model.setSourceModelSummary(modelSummary);
            return this;
        }

        public Builder dedupTargetTableName(String targetTableName) {
            dedupEventTable.setTargetTableName(targetTableName);
            match.setInputTableName(targetTableName);
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

        public Builder matchClientDocument(MatchClientDocument matchClientDocument) {
            match.setDbUrl(matchClientDocument.getUrl());
            match.setDbUser(matchClientDocument.getUsername());
            match.setDbPasswordEncrypted(matchClientDocument.getEncryptedPassword());
            match.setMatchClient(matchClientDocument.getMatchClient().name());
            return this;
        }

        public Builder excludePublicDomains(boolean excludePublicDomains) {
            match.setExcludePublicDomains(excludePublicDomains);
            model.addProvenanceProperty(ProvenancePropertyName.ExcludePublicDomains, excludePublicDomains);
            return this;
        }

        public Builder addProvenanceProperty(ProvenancePropertyName propertyName, Object value) {
            model.addProvenanceProperty(propertyName, value);
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

        public Builder dataCloudVersion(String dataCloudVersion) {
            match.setDataCloudVersion(dataCloudVersion);
            matchResult.setDataCloudVersion(dataCloudVersion);
            model.setDataCloudVersion(dataCloudVersion);
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

        public Builder userRefinedAttributes(List<Attribute> userRefinedAttributes) {
            resolveAttributes.setUserRefinedAttributes(userRefinedAttributes);
            return this;
        }

        public Builder pivotArtifactPath(String pivotArtifactPath) {
            model.setPivotArtifactPath(pivotArtifactPath);
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

        public Builder moduleName(String moduleName) {
            model.setModuleName(moduleName);
            return this;
        }

        public Builder enableV2Profiling(boolean v2ProfilingEnabled) {
            model.setV2ProfilingEnabled(v2ProfilingEnabled);
            return this;
        }

        public MatchAndModelWorkflowConfiguration build() {
            match.setMatchQueue(LedpQueueAssigner.getModelingQueueNameForSubmission());
            export.setUsingDisplayName(Boolean.FALSE);
            export.setExportDestination(ExportDestination.FILE);
            export.setExportFormat(ExportFormat.CSV);

            configuration.add(dedupEventTable);
            configuration.add(match);
            configuration.add(model);
            configuration.add(export);
            configuration.add(matchResult);
            configuration.add(addStandardAttributes);
            configuration.add(resolveAttributes);
            configuration.add(setConfigForScoring);
            configuration.add(score);
            configuration.add(combineInputWithScores);
            configuration.add(pivotScoreAndEvent);

            return configuration;
        }

    }
}
