package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.ModelStepConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class CdlModelWorkflowConfiguration extends BaseCDLWorkflowConfiguration {
    private static Logger log = LoggerFactory.getLogger(CdlModelWorkflowConfiguration.class);

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Modeling.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {
        private CdlModelWorkflowConfiguration configuration = new CdlModelWorkflowConfiguration();
        private ModelStepConfiguration model = new ModelStepConfiguration();
        private ExportStepConfiguration export = new ExportStepConfiguration();

        public Builder microServiceHostPort(String microServiceHostPort) {
            model.setMicroServiceHostPort(microServiceHostPort);
            export.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            model.setInternalResourceHostPort(internalResourceHostPort);
            export.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            model.setCustomerSpace(customerSpace);
            export.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder workflow(String workflowName) {
            configuration.setWorkflowName(workflowName);
            configuration.setName(configuration.getClass().getSimpleName());
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

        public Builder targetTableName(String targetTableName) {
            model.setTargetTableName(targetTableName);
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

        public Builder setCrossSellModel(boolean isCrossSellModel) {
            model.setCrossSellModel(isCrossSellModel);
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

        public Builder aiModelId(String aiModelId) {
            model.setAiModelId(aiModelId);
            return this;
        }

        public Builder ratingEngineId(String ratingEngineId) {
            model.setRatingEngineId(ratingEngineId);
            return this;
        }

        public Builder setExpectedValue(boolean expectedValue) {
            model.setExpectedValue(expectedValue);
            return this;
        }

        public Builder idColumnName(String idColumnName) {
            model.setIdColumnName(idColumnName);
            return this;
        }

        public CdlModelWorkflowConfiguration build() {
            export.setUsingDisplayName(Boolean.FALSE);
            export.setExportDestination(ExportDestination.FILE);
            export.setExportFormat(ExportFormat.CSV);

            configuration.setContainerConfiguration("cdlModelWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(model);
            configuration.add(export);
            log.info(String.format("Build configuration (type %s) for model workflow: %s",
                    CdlModelWorkflowConfiguration.class.getSimpleName(),
                    JsonUtils.serialize(configuration)));
            return configuration;
        }

    }
}
