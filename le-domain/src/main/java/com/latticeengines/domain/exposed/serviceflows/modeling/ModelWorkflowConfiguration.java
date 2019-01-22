package com.latticeengines.domain.exposed.serviceflows.modeling;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.cdl.CdlModelWorkflowConfiguration.Builder;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.ModelStepConfiguration;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class ModelWorkflowConfiguration extends BaseModelingWorkflowConfiguration {

    public static class Builder {
        private ModelWorkflowConfiguration configuration = new ModelWorkflowConfiguration();

        private ModelStepConfiguration model = new ModelStepConfiguration();
        private ExportStepConfiguration export = new ExportStepConfiguration();

        public Builder microServiceHostPort(String microServiceHostPort) {
            model.setMicroServiceHostPort(microServiceHostPort);
            export.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            model.setInternalResourceHostPort(internalResourceHostPort);
            export.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            model.setCustomerSpace(customerSpace);
            export.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder modelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            model.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
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
            return this;
        }

        public Builder userId(String userId) {
            model.setUserName(userId);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public Builder transformationGroup(TransformationGroup transformationGroup,
                List<TransformDefinition> stdTransformDefns) {
            model.addProvenanceProperty(ProvenancePropertyName.TransformationGroupName, transformationGroup.getName());
            return this;
        }

        public Builder sourceModelSummary(ModelSummary modelSummary) {
            model.setSourceModelSummary(modelSummary);
            return this;
        }

        public Builder dedupType(DedupType dedupType) {
            return this;
        }

        public Builder dedupDataFlowBeanName(String beanName) {
            return this;
        }

        public Builder excludeDataCloudAttrs(boolean exclude) {
            model.addProvenanceProperty(ProvenancePropertyName.ExcludePropdataColumns, exclude);
            return this;
        }

        public Builder skipDedupStep(boolean skipDedupStep) {
            model.addProvenanceProperty(ProvenancePropertyName.IsOneLeadPerDomain, !skipDedupStep);
            return this;
        }

        public Builder excludePublicDomain(boolean excludePublicDomains) {
            model.addProvenanceProperty(ProvenancePropertyName.ExcludePublicDomains, excludePublicDomains);
            return this;
        }

        public Builder addProvenanceProperty(ProvenancePropertyName propertyName, Object value) {
            model.addProvenanceProperty(propertyName, value);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            model.setDataCloudVersion(dataCloudVersion);
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
        
        // FIX THIS... custom event wf needs to have event columns name
        public Builder setEventColumn(String eventColumn) {
            model.setEventColumn(eventColumn);
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

        public Builder notesContent(String notesContent) {
            model.setNotesContent(notesContent);
            return this;
        }

        public Builder setActivateModelSummaryByDefault(boolean value) {
            model.setActivateModelSummaryByDefault(value);
            return this;
        }

        public Builder runTimeParams(Map<String, String> runTimeParams) {
            model.setRunTimeParams(runTimeParams);
            return this;
        }

        public Builder targetTableName(String targetTableName) {
            model.setTargetTableName(targetTableName);
            return this;
        }

        public Builder aiModelId(String aiModelId) {
            model.setAiModelId(aiModelId);
            return this;
        }

        public Builder ratingEngineId(String ratingEngineId) {
            model.setRatingEngineId(ratingEngineId);
            return this;
        }

        public Builder idColumnName(String idColumnName) {
            model.setIdColumnName(idColumnName);
            return this;
        }

        public ModelWorkflowConfiguration build() {
            export.setUsingDisplayName(Boolean.FALSE);

            configuration.setContainerConfiguration("modelWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(model);
            configuration.add(export);
            return configuration;
        }

        public Builder skipStandardTransform(boolean skipStandardTransform) {
            model.setSkipStandardTransform(skipStandardTransform);
            return this;
        }
    }
}
