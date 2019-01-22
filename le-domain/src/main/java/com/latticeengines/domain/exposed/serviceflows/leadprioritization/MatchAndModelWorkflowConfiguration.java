package com.latticeengines.domain.exposed.serviceflows.leadprioritization;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.core.steps.AddStandardAttributesConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.MatchDataCloudWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.ResolveMetadataFromUserRefinedAttributesConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.ModelWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.DedupEventTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.RTSBulkScoreWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ComputeLiftDataFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ExportBucketToolStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.PivotScoreAndEventConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.SetConfigurationForScoringConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class MatchAndModelWorkflowConfiguration extends BaseLPWorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Modeling.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {
        private MatchAndModelWorkflowConfiguration configuration = new MatchAndModelWorkflowConfiguration();
        private MatchDataCloudWorkflowConfiguration.Builder matchDataCloudWorkflowBuilder = new MatchDataCloudWorkflowConfiguration.Builder();

        private DedupEventTableConfiguration dedupEventTable = new DedupEventTableConfiguration();
        private AddStandardAttributesConfiguration addStandardAttributes = new AddStandardAttributesConfiguration();
        private ResolveMetadataFromUserRefinedAttributesConfiguration resolveAttributes = new ResolveMetadataFromUserRefinedAttributesConfiguration();

        private ModelWorkflowConfiguration.Builder modelWorkflowBuilder = new ModelWorkflowConfiguration.Builder();
        private SetConfigurationForScoringConfiguration setConfigForScoring = new SetConfigurationForScoringConfiguration();
        private RTSBulkScoreWorkflowConfiguration.Builder rtsBulkScoreWorkflowBuilder = new RTSBulkScoreWorkflowConfiguration.Builder();
        private ComputeLiftDataFlowConfiguration computeLift = new ComputeLiftDataFlowConfiguration();
        private PivotScoreAndEventConfiguration pivotScoreAndEvent = new PivotScoreAndEventConfiguration();
        private ExportBucketToolStepConfiguration export = new ExportBucketToolStepConfiguration();
        private ImportExportS3StepConfiguration modelExportToS3 = new ImportExportS3StepConfiguration();

        public Builder microServiceHostPort(String microServiceHostPort) {
            dedupEventTable.setMicroServiceHostPort(microServiceHostPort);
            matchDataCloudWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            modelWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            addStandardAttributes.setMicroServiceHostPort(microServiceHostPort);
            resolveAttributes.setMicroServiceHostPort(microServiceHostPort);
            setConfigForScoring.setMicroServiceHostPort(microServiceHostPort);
            rtsBulkScoreWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            pivotScoreAndEvent.setMicroServiceHostPort(microServiceHostPort);
            export.setMicroServiceHostPort(microServiceHostPort);
            computeLift.setMicroServiceHostPort(microServiceHostPort);
            modelExportToS3.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            dedupEventTable.setInternalResourceHostPort(internalResourceHostPort);
            matchDataCloudWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            modelWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            addStandardAttributes.setInternalResourceHostPort(internalResourceHostPort);
            resolveAttributes.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            setConfigForScoring.setInternalResourceHostPort(internalResourceHostPort);
            rtsBulkScoreWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            pivotScoreAndEvent.setInternalResourceHostPort(internalResourceHostPort);
            export.setInternalResourceHostPort(internalResourceHostPort);
            modelExportToS3.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            dedupEventTable.setCustomerSpace(customerSpace);
            matchDataCloudWorkflowBuilder.customer(customerSpace);
            modelWorkflowBuilder.customer(customerSpace);
            addStandardAttributes.setCustomerSpace(customerSpace);
            resolveAttributes.setCustomerSpace(customerSpace);
            setConfigForScoring.setCustomerSpace(customerSpace);
            rtsBulkScoreWorkflowBuilder.customer(customerSpace);
            pivotScoreAndEvent.setCustomerSpace(customerSpace);
            computeLift.setCustomerSpace(customerSpace);
            export.setCustomerSpace(customerSpace);
            modelExportToS3.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder modelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            modelWorkflowBuilder.modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            setConfigForScoring.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
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

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            matchDataCloudWorkflowBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            rtsBulkScoreWorkflowBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            modelWorkflowBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            addStandardAttributes.setSourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public Builder trainingTableName(String trainingTableName) {
            matchDataCloudWorkflowBuilder.matchInputTableName(trainingTableName);
            modelWorkflowBuilder.trainingTableName(trainingTableName);
            rtsBulkScoreWorkflowBuilder.inputTableName(trainingTableName);
            return this;
        }

        public Builder userId(String userId) {
            computeLift.setUserId(userId);
            modelWorkflowBuilder.userId(userId);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            setConfigForScoring.setInputProperties(inputProperties);
            return this;
        }

        public Builder transformationGroup(TransformationGroup transformationGroup,
                List<TransformDefinition> stdTransformDefns) {
            addStandardAttributes.setTransformationGroup(transformationGroup);
            addStandardAttributes.setTransforms(stdTransformDefns);
            modelWorkflowBuilder.addProvenanceProperty(ProvenancePropertyName.TransformationGroupName,
                    transformationGroup.getName());
            return this;
        }

        public Builder sourceModelSummary(ModelSummary modelSummary) {
            modelWorkflowBuilder.sourceModelSummary(modelSummary);
            return this;
        }

        public Builder dedupType(DedupType dedupType) {
            dedupEventTable.setDedupType(dedupType);
            return this;
        }

        public Builder dedupDataFlowBeanName(String beanName) {
            dedupEventTable.setBeanName(beanName);
            return this;
        }

        public Builder excludeDataCloudAttrs(boolean exclude) {
            matchDataCloudWorkflowBuilder.excludeDataCloudAttrs(exclude);
            rtsBulkScoreWorkflowBuilder.excludeDataCloudAttrs(exclude);
            modelWorkflowBuilder.excludeDataCloudAttrs(exclude);
            return this;
        }

        public Builder skipDedupStep(boolean skipDedupStep) {
            matchDataCloudWorkflowBuilder.skipDedupStep(skipDedupStep);
            dedupEventTable.setSkipStep(skipDedupStep);
            modelWorkflowBuilder.skipDedupStep(skipDedupStep);
            return this;
        }

        public Builder skipStandardTransform(boolean skipTransform) {
            addStandardAttributes.setSkipStep(skipTransform);
            return this;
        }

        public Builder matchClientDocument(MatchClientDocument matchClientDocument) {
            matchDataCloudWorkflowBuilder.matchClientDocument(matchClientDocument);
            rtsBulkScoreWorkflowBuilder.matchClientDocument(matchClientDocument);
            return this;
        }

        public Builder excludePublicDomain(boolean excludePublicDomains) {
            matchDataCloudWorkflowBuilder.excludePublicDomains(excludePublicDomains);
            rtsBulkScoreWorkflowBuilder.excludeDataCloudAttrs(excludePublicDomains);
            modelWorkflowBuilder.excludePublicDomain(excludePublicDomains);
            return this;
        }

        public Builder setRetainLatticeAccountId(boolean retainLatticeAccountId) {
            matchDataCloudWorkflowBuilder.setRetainLatticeAccountId(retainLatticeAccountId);
            rtsBulkScoreWorkflowBuilder.setRetainLatticeAccountId(retainLatticeAccountId);
            return this;
        }

        public Builder treatPublicDomainAsNormalDomain(boolean publicDomainAsNormalDomain) {
            matchDataCloudWorkflowBuilder.treatPublicDomainAsNormalDomain(publicDomainAsNormalDomain);
            rtsBulkScoreWorkflowBuilder.treatPublicDomainAsNormalDomain(publicDomainAsNormalDomain);
            return this;
        }

        public Builder addProvenanceProperty(ProvenancePropertyName propertyName, Object value) {
            modelWorkflowBuilder.addProvenanceProperty(propertyName, value);
            return this;
        }

        public Builder matchType(MatchCommandType matchCommandType) {
            matchDataCloudWorkflowBuilder.matchType(matchCommandType);
            rtsBulkScoreWorkflowBuilder.matchType(matchCommandType);
            return this;
        }

        public Builder matchDestTables(String destTables) {
            matchDataCloudWorkflowBuilder.matchDestTables(destTables);
            rtsBulkScoreWorkflowBuilder.matchDestTables(destTables);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            matchDataCloudWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            rtsBulkScoreWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            modelWorkflowBuilder.dataCloudVersion(dataCloudVersion);
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
            matchDataCloudWorkflowBuilder.matchColumnSelection(predefinedColumnSelection, selectionVersion);
            rtsBulkScoreWorkflowBuilder.matchColumnSelection(predefinedColumnSelection, selectionVersion);
            return this;
        }

        public Builder matchDebugEnabled(boolean matchDebugEnabled) {
            rtsBulkScoreWorkflowBuilder.matchDebugEnabled(matchDebugEnabled);
            return this;
        }

        public Builder matchRequestSource(MatchRequestSource matchRequestSource) {
            matchDataCloudWorkflowBuilder.matchRequestSource(matchRequestSource);
            rtsBulkScoreWorkflowBuilder.matchRequestSource(matchRequestSource);
            return this;
        }

        public Builder userRefinedAttributes(List<Attribute> userRefinedAttributes) {
            resolveAttributes.setUserRefinedAttributes(userRefinedAttributes);
            return this;
        }

        public Builder pivotArtifactPath(String pivotArtifactPath) {
            modelWorkflowBuilder.pivotArtifactPath(pivotArtifactPath);
            return this;
        }

        public Builder dataRules(List<DataRule> dataRules) {
            modelWorkflowBuilder.dataRules(dataRules);
            return this;
        }

        public Builder eventColumn(String eventColumn) {
            modelWorkflowBuilder.setEventColumn(eventColumn);
            return this;
        }

        public Builder isDefaultDataRules(boolean isDefaultDataRules) {
            modelWorkflowBuilder.isDefaultDataRules(isDefaultDataRules);
            return this;
        }

        public Builder moduleName(String moduleName) {
            modelWorkflowBuilder.moduleName(moduleName);
            return this;
        }

        public Builder enableV2Profiling(boolean v2ProfilingEnabled) {
            modelWorkflowBuilder.enableV2Profiling(v2ProfilingEnabled);
            return this;
        }

        public Builder notesContent(String notesContent) {
            modelWorkflowBuilder.notesContent(notesContent);
            return this;
        }

        public Builder matchQueue(String queue) {
            matchDataCloudWorkflowBuilder.matchQueue(queue);
            rtsBulkScoreWorkflowBuilder.matchQueue(queue);
            return this;
        }

        public Builder enableLeadEnrichment(boolean enableLeadEnrichment) {
            rtsBulkScoreWorkflowBuilder.enableLeadEnrichment(enableLeadEnrichment);
            return this;
        }

        public Builder setScoreTestFile(boolean scoreTestFile) {
            rtsBulkScoreWorkflowBuilder.setScoreTestFile(scoreTestFile);
            return this;
        }

        public Builder enableDebug(boolean enableDebug) {
            rtsBulkScoreWorkflowBuilder.enableDebug(enableDebug);
            return this;
        }

        public Builder modelType(String modelType) {
            rtsBulkScoreWorkflowBuilder.modelType(modelType);
            return this;
        }

        public Builder setActivateModelSummaryByDefault(boolean value) {
            modelWorkflowBuilder.setActivateModelSummaryByDefault(value);
            return this;
        }

        public Builder runTimeParams(Map<String, String> runTimeParams) {
            modelWorkflowBuilder.runTimeParams(runTimeParams);
            addStandardAttributes.setRuntimeParams(runTimeParams);
            return this;
        }

        public Builder bucketMetadata(List<BucketMetadata> bucketMetadata) {
            rtsBulkScoreWorkflowBuilder.bucketMetadata(bucketMetadata);
            return this;
        }

        public MatchAndModelWorkflowConfiguration build() {
            configuration.setContainerConfiguration("modelAndEmailWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            export.setUsingDisplayName(Boolean.FALSE);
            computeLift.setScoreField(InterfaceName.Event.name());
            computeLift.setSaveBucketMetadata(Boolean.TRUE);
            pivotScoreAndEvent.setSaveBucketMetadata(Boolean.TRUE);
            pivotScoreAndEvent.setScoreField(InterfaceName.Event.name());

            configuration.add(dedupEventTable);
            configuration.add(matchDataCloudWorkflowBuilder.build());
            configuration.add(modelWorkflowBuilder.build());
            configuration.add(rtsBulkScoreWorkflowBuilder.build());
            configuration.add(addStandardAttributes);
            configuration.add(resolveAttributes);
            configuration.add(setConfigForScoring);
            configuration.add(computeLift);
            configuration.add(pivotScoreAndEvent);
            configuration.add(export);
            configuration.add(modelExportToS3);
            return configuration;
        }

    }
}
