package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.LdcOnlyAttributesConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.AddStandardAttributesConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.BaseReportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.ModelDataValidationWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.ModelWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.DedupEventTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.RTSBulkScoreWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ComputeLiftDataFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.PivotScoreAndEventConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class CustomEventModelingWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Modeling.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {
        private CustomEventModelingWorkflowConfiguration configuration = new CustomEventModelingWorkflowConfiguration();

        private ImportStepConfiguration importData = new ImportStepConfiguration();
        private BaseReportStepConfiguration registerReport = new BaseReportStepConfiguration();
        private ModelDataValidationWorkflowConfiguration.Builder modelDataValidationWorkflow = new ModelDataValidationWorkflowConfiguration.Builder();

        private CustomEventMatchWorkflowConfiguration.Builder customEventMatchWorkflowConfigurationBuilder = new CustomEventMatchWorkflowConfiguration.Builder();

        private DedupEventTableConfiguration dedupEventTable = new DedupEventTableConfiguration();
        private AddStandardAttributesConfiguration addStandardAttributes = new AddStandardAttributesConfiguration();

        private ModelWorkflowConfiguration.Builder modelWorkflowBuilder = new ModelWorkflowConfiguration.Builder();

        private PrepareScoringAfterModelingWorkflowConfiguration.Builder prepareConfigForScoringBuilder = new PrepareScoringAfterModelingWorkflowConfiguration.Builder();
        private RTSBulkScoreWorkflowConfiguration.Builder rtsBulkScoreWorkflowBuilder = new RTSBulkScoreWorkflowConfiguration.Builder();

        private ComputeLiftDataFlowConfiguration computeLift = new ComputeLiftDataFlowConfiguration();
        private PivotScoreAndEventConfiguration pivotScoreAndEvent = new PivotScoreAndEventConfiguration();
        private ExportStepConfiguration export = new ExportStepConfiguration();
        private LdcOnlyAttributesConfiguration ldcOnlyAttributes = new LdcOnlyAttributesConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            customEventMatchWorkflowConfigurationBuilder.customer(customerSpace);
            importData.setCustomerSpace(customerSpace);
            registerReport.setCustomerSpace(customerSpace);
            modelDataValidationWorkflow.customer(customerSpace);
            dedupEventTable.setCustomerSpace(customerSpace);
            addStandardAttributes.setCustomerSpace(customerSpace);
            ldcOnlyAttributes.setCustomerSpace(customerSpace);
            export.setCustomerSpace(customerSpace);
            modelWorkflowBuilder.customer(customerSpace);
            prepareConfigForScoringBuilder.customer(customerSpace);
            rtsBulkScoreWorkflowBuilder.customer(customerSpace);
            computeLift.setCustomerSpace(customerSpace);
            pivotScoreAndEvent.setCustomerSpace(customerSpace);

            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            customEventMatchWorkflowConfigurationBuilder.microServiceHostPort(microServiceHostPort);
            importData.setMicroServiceHostPort(microServiceHostPort);
            registerReport.setMicroServiceHostPort(microServiceHostPort);
            modelDataValidationWorkflow.microServiceHostPort(microServiceHostPort);
            dedupEventTable.setMicroServiceHostPort(microServiceHostPort);

            addStandardAttributes.setMicroServiceHostPort(microServiceHostPort);
            ldcOnlyAttributes.setMicroServiceHostPort(microServiceHostPort);
            modelWorkflowBuilder.microServiceHostPort(microServiceHostPort);

            rtsBulkScoreWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            prepareConfigForScoringBuilder.microServiceHostPort(microServiceHostPort);
            computeLift.setMicroServiceHostPort(microServiceHostPort);
            pivotScoreAndEvent.setMicroServiceHostPort(microServiceHostPort);
            export.setMicroServiceHostPort(microServiceHostPort);

            return this;
        }

        public Builder sourceFileName(String sourceFileName) {
            importData.setSourceFileName(sourceFileName);
            return this;
        }

        public Builder dedupType(DedupType dedupType) {
            dedupEventTable.setDedupType(dedupType);
            return this;
        }

        public Builder minPositiveEvents(long minPositiveEvents) {
            modelDataValidationWorkflow.minPositiveEvents(minPositiveEvents);
            return this;
        }

        public Builder minNegativeEvents(long minNegativeEvents) {
            modelDataValidationWorkflow.minNegativeEvents(minNegativeEvents);
            return this;
        }

        public Builder minRows(long minRows) {
            modelDataValidationWorkflow.minRows(minRows);
            return this;
        }

        public Builder sourceType(SourceType sourceType) {
            importData.setSourceType(sourceType);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            customEventMatchWorkflowConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
            importData.setInternalResourceHostPort(internalResourceHostPort);
            registerReport.setInternalResourceHostPort(internalResourceHostPort);
            modelDataValidationWorkflow.internalResourceHostPort(internalResourceHostPort);
            dedupEventTable.setInternalResourceHostPort(internalResourceHostPort);
            modelWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            addStandardAttributes.setInternalResourceHostPort(internalResourceHostPort);
            ldcOnlyAttributes.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            prepareConfigForScoringBuilder.internalResourceHostPort(internalResourceHostPort);
            rtsBulkScoreWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            pivotScoreAndEvent.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder importReportNamePrefix(String reportName) {
            registerReport.setReportNamePrefix(reportName);
            return this;
        }

        public Builder eventTableReportNamePrefix(String eventTableReportName) {
            modelDataValidationWorkflow.eventTableReportNamePrefix(eventTableReportName);
            return this;
        }

        public Builder dedupDataFlowBeanName(String beanName) {
            dedupEventTable.setBeanName(beanName);
            return this;
        }

        public Builder userId(String userId) {
            pivotScoreAndEvent.setUserId(userId);
            modelWorkflowBuilder.userId(userId);
            configuration.setUserId(userId);
            return this;
        }

        public Builder modelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            modelWorkflowBuilder.modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            prepareConfigForScoringBuilder.modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            prepareConfigForScoringBuilder.inputProperties(inputProperties);
            return this;
        }

        public Builder matchClientDocument(MatchClientDocument matchClientDocument) {
            customEventMatchWorkflowConfigurationBuilder.matchClientDocument(matchClientDocument);
            prepareConfigForScoringBuilder.matchClientDocument(matchClientDocument);
            rtsBulkScoreWorkflowBuilder.matchClientDocument(matchClientDocument);
            return this;
        }

        public Builder matchAccountIdColumn(String matchAccountIdColumn) {
            customEventMatchWorkflowConfigurationBuilder.matchAccountIdColumn(matchAccountIdColumn);
            prepareConfigForScoringBuilder.matchAccountIdColumn(matchAccountIdColumn);
            return this;
        }

        public Builder modelingType(CustomEventModelingType customEventModelingType) {
            customEventMatchWorkflowConfigurationBuilder.modelingType(customEventModelingType);
            prepareConfigForScoringBuilder.modelingType(customEventModelingType);
            if (CustomEventModelingType.LPI == customEventModelingType) {
                computeLift.setScoreField(InterfaceName.Event.name());
            } else if (CustomEventModelingType.CDL == customEventModelingType) {
                computeLift.setScoreField(ScoreResultField.Percentile.displayName);
                rtsBulkScoreWorkflowBuilder.skipMatching(Boolean.TRUE);
                rtsBulkScoreWorkflowBuilder.setScoreTestFile(Boolean.TRUE);
            }
            return this;
        }

        public Builder targetTableName(String targetTableName) {
            modelWorkflowBuilder.targetTableName(targetTableName);
            prepareConfigForScoringBuilder.matchCdlTargetTableName(targetTableName);
            return this;
        }

        public Builder metadataSegmentExport(MetadataSegmentExport metadataSegmentExport) {
            prepareConfigForScoringBuilder.metadataSegmentExport(metadataSegmentExport);
            return this;
        }

        public Builder aiModelId(String aiModelId) {
            modelWorkflowBuilder.aiModelId(aiModelId);
            return this;
        }

        public Builder ratingEngineId(String ratingEngineId) {
            modelWorkflowBuilder.ratingEngineId(ratingEngineId);
            return this;
        }

        public Builder matchRequestSource(MatchRequestSource matchRequestSource) {
            customEventMatchWorkflowConfigurationBuilder.matchRequestSource(matchRequestSource);
            prepareConfigForScoringBuilder.matchRequestSource(matchRequestSource);
            rtsBulkScoreWorkflowBuilder.matchRequestSource(matchRequestSource);
            return this;
        }

        public Builder matchQueue(String queue) {
            customEventMatchWorkflowConfigurationBuilder.matchQueue(queue);
            prepareConfigForScoringBuilder.matchQueue(queue);
            rtsBulkScoreWorkflowBuilder.matchQueue(queue);
            return this;
        }

        public Builder fetchOnly(boolean fetchOnly) {
            customEventMatchWorkflowConfigurationBuilder.fetchOnly(fetchOnly);
            rtsBulkScoreWorkflowBuilder.fetchOnly(fetchOnly);
            prepareConfigForScoringBuilder.fetchOnly(fetchOnly);
            return this;
        }

        public Builder matchColumnSelection(Predefined predefinedColumnSelection, String selectionVersion) {
            customEventMatchWorkflowConfigurationBuilder.matchColumnSelection(predefinedColumnSelection,
                    selectionVersion);
            prepareConfigForScoringBuilder.matchColumnSelection(predefinedColumnSelection, selectionVersion);
            rtsBulkScoreWorkflowBuilder.matchColumnSelection(predefinedColumnSelection, selectionVersion);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            customEventMatchWorkflowConfigurationBuilder.dataCloudVersion(dataCloudVersion);
            modelWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            prepareConfigForScoringBuilder.dataCloudVersion(dataCloudVersion);
            rtsBulkScoreWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchType(MatchCommandType matchCommandType) {
            customEventMatchWorkflowConfigurationBuilder.matchType(matchCommandType);
            prepareConfigForScoringBuilder.matchType(matchCommandType);
            rtsBulkScoreWorkflowBuilder.matchType(matchCommandType);
            return this;
        }

        public Builder setRetainLatticeAccountId(boolean retainLatticeAccountId) {
            customEventMatchWorkflowConfigurationBuilder.setRetainLatticeAccountId(retainLatticeAccountId);
            prepareConfigForScoringBuilder.setRetainLatticeAccountId(retainLatticeAccountId);
            rtsBulkScoreWorkflowBuilder.setRetainLatticeAccountId(retainLatticeAccountId);
            return this;
        }

        public Builder matchDestTables(String destTables) {
            customEventMatchWorkflowConfigurationBuilder.matchDestTables(destTables);
            prepareConfigForScoringBuilder.matchDestTables(destTables);
            rtsBulkScoreWorkflowBuilder.matchDestTables(destTables);
            return this;
        }

        public Builder excludePublicDomains(boolean excludePublicDomains) {
            customEventMatchWorkflowConfigurationBuilder.excludePublicDomains(excludePublicDomains);
            modelWorkflowBuilder.excludePublicDomain(excludePublicDomains);
            prepareConfigForScoringBuilder.excludePublicDomains(excludePublicDomains);
            rtsBulkScoreWorkflowBuilder.excludePublicDomain(excludePublicDomains);
            return this;
        }

        public Builder excludeDataCloudAttrs(boolean exclude) {
            customEventMatchWorkflowConfigurationBuilder.excludeDataCloudAttrs(exclude);
            modelWorkflowBuilder.excludeDataCloudAttrs(exclude);
            rtsBulkScoreWorkflowBuilder.excludeDataCloudAttrs(exclude);
            prepareConfigForScoringBuilder.excludeDataCloudAttrs(exclude);
            return this;
        }

        public Builder skipDedupStep(boolean skipDedupStep) {
            customEventMatchWorkflowConfigurationBuilder.skipDedupStep(skipDedupStep);
            dedupEventTable.setSkipStep(skipDedupStep);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            customEventMatchWorkflowConfigurationBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            addStandardAttributes.setSourceSchemaInterpretation(sourceSchemaInterpretation);
            modelWorkflowBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            prepareConfigForScoringBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            rtsBulkScoreWorkflowBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public Builder matchDebugEnabled(boolean matchDebugEnabled) {
            rtsBulkScoreWorkflowBuilder.matchDebugEnabled(matchDebugEnabled);
            return this;
        }

        public Builder skipStandardTransform(boolean skipTransform) {
            addStandardAttributes.setSkipStep(skipTransform);
            return this;
        }

        public Builder modelName(String modelName) {
            modelWorkflowBuilder.modelName(modelName);
            return this;
        }

        public Builder displayName(String displayName) {
            modelWorkflowBuilder.displayName(displayName);
            return this;
        }

        public Builder trainingTableName(String trainingTableName) {
            modelDataValidationWorkflow.sourceTableName(trainingTableName);
            customEventMatchWorkflowConfigurationBuilder.matchInputTableName(trainingTableName);
            modelWorkflowBuilder.trainingTableName(trainingTableName);
            rtsBulkScoreWorkflowBuilder.inputTableName(trainingTableName);
            return this;
        }

        public Builder transformationGroup(TransformationGroup transformationGroup,
                List<TransformDefinition> stdTransformDefns) {
            addStandardAttributes.setTransformationGroup(transformationGroup);
            addStandardAttributes.setTransforms(stdTransformDefns);
            modelWorkflowBuilder.transformationGroup(transformationGroup, stdTransformDefns);
            return this;
        }

        public Builder enableV2Profiling(boolean v2ProfilingEnabled) {
            modelWorkflowBuilder.enableV2Profiling(v2ProfilingEnabled);
            return this;
        }

        public Builder addProvenanceProperty(ProvenancePropertyName propertyName, Object value) {
            modelWorkflowBuilder.addProvenanceProperty(propertyName, value);
            return this;
        }

        public Builder pivotArtifactPath(String pivotArtifactPath) {
            modelWorkflowBuilder.pivotArtifactPath(pivotArtifactPath);
            return this;
        }

        public Builder moduleName(String moduleName) {
            modelWorkflowBuilder.moduleName(moduleName);
            return this;
        }

        public Builder runTimeParams(Map<String, String> runTimeParams) {
            modelWorkflowBuilder.runTimeParams(runTimeParams);
            addStandardAttributes.setRuntimeParams(runTimeParams);
            return this;
        }

        public Builder dataRules(List<DataRule> dataRules) {
            modelWorkflowBuilder.dataRules(dataRules);
            return this;
        }

        public Builder isDefaultDataRules(boolean isDefaultDataRules) {
            modelWorkflowBuilder.isDefaultDataRules(isDefaultDataRules);
            return this;
        }

        public Builder bucketMetadata(List<BucketMetadata> bucketMetadata) {
            rtsBulkScoreWorkflowBuilder.bucketMetadata(bucketMetadata);
            return this;
        }

        public Builder setActivateModelSummaryByDefault(boolean value) {
            modelWorkflowBuilder.setActivateModelSummaryByDefault(value);
            return this;
        }

        public Builder skipLdcAttributesOnly(boolean skipLdcAttributesOnly) {
            ldcOnlyAttributes.setSkipStep(skipLdcAttributesOnly);
            return this;
        }

        public Builder notesContent(String notesContent) {
            modelWorkflowBuilder.notesContent(notesContent);
            return this;
        }

        public Builder idColumnName(String idColumnName) {
            modelWorkflowBuilder.idColumnName(idColumnName);
            rtsBulkScoreWorkflowBuilder.idColumnName(idColumnName);
            return this;
        }

        public CustomEventModelingWorkflowConfiguration build() {
            configuration.setContainerConfiguration("customEventModelingWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());

            configuration.add(importData);
            configuration.add(registerReport);
            configuration.add(modelDataValidationWorkflow.build());
            configuration.add(customEventMatchWorkflowConfigurationBuilder.build());

            configuration.add(dedupEventTable);
            configuration.add(addStandardAttributes);
            configuration.add(ldcOnlyAttributes);
            configuration.add(modelWorkflowBuilder.build());
            configuration.add(prepareConfigForScoringBuilder.build());
            configuration.add(rtsBulkScoreWorkflowBuilder.build());
            configuration.add(computeLift);
            configuration.add(pivotScoreAndEvent);
            configuration.add(export);

            return configuration;
        }
    }
}
