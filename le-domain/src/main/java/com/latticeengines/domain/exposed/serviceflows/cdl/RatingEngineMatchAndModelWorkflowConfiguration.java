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
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.core.steps.AddStandardAttributesConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.MatchDataCloudWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.ResolveMetadataFromUserRefinedAttributesConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.DedupEventTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.PivotScoreAndEventConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.SetConfigurationForScoringConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class RatingEngineMatchAndModelWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Modeling.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {
        private RatingEngineMatchAndModelWorkflowConfiguration configuration = new RatingEngineMatchAndModelWorkflowConfiguration();
        private MatchDataCloudWorkflowConfiguration.Builder matchDataCloudWorkflowBuilder = new MatchDataCloudWorkflowConfiguration.Builder();
        private CdlModelWorkflowConfiguration.Builder cdlModelWorkflowBuilder = new CdlModelWorkflowConfiguration.Builder();
        private RatingEngineScoreWorkflowConfiguration.Builder ratingEngineScoreWorkflowBuilder = new RatingEngineScoreWorkflowConfiguration.Builder();

        private DedupEventTableConfiguration dedupEventTable = new DedupEventTableConfiguration();
        private AddStandardAttributesConfiguration addStandardAttributes = new AddStandardAttributesConfiguration();
        private ResolveMetadataFromUserRefinedAttributesConfiguration resolveAttributes = new ResolveMetadataFromUserRefinedAttributesConfiguration();

        private SetConfigurationForScoringConfiguration setConfigForScoring = new SetConfigurationForScoringConfiguration();
        private PivotScoreAndEventConfiguration pivotScoreAndEvent = new PivotScoreAndEventConfiguration();
        private ExportStepConfiguration export = new ExportStepConfiguration();

        public Builder microServiceHostPort(String microServiceHostPort) {
            dedupEventTable.setMicroServiceHostPort(microServiceHostPort);
            matchDataCloudWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            cdlModelWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            addStandardAttributes.setMicroServiceHostPort(microServiceHostPort);
            resolveAttributes.setMicroServiceHostPort(microServiceHostPort);
            setConfigForScoring.setMicroServiceHostPort(microServiceHostPort);
            ratingEngineScoreWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            pivotScoreAndEvent.setMicroServiceHostPort(microServiceHostPort);
            export.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            dedupEventTable.setInternalResourceHostPort(internalResourceHostPort);
            matchDataCloudWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            cdlModelWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            addStandardAttributes.setInternalResourceHostPort(internalResourceHostPort);
            resolveAttributes.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            setConfigForScoring.setInternalResourceHostPort(internalResourceHostPort);
            ratingEngineScoreWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            pivotScoreAndEvent.setInternalResourceHostPort(internalResourceHostPort);
            export.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            dedupEventTable.setCustomerSpace(customerSpace);
            matchDataCloudWorkflowBuilder.customer(customerSpace);
            cdlModelWorkflowBuilder.customer(customerSpace);
            addStandardAttributes.setCustomerSpace(customerSpace);
            resolveAttributes.setCustomerSpace(customerSpace);
            setConfigForScoring.setCustomerSpace(customerSpace);
            ratingEngineScoreWorkflowBuilder.customer(customerSpace);
            pivotScoreAndEvent.setCustomerSpace(customerSpace);
            export.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder modelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            cdlModelWorkflowBuilder.modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            setConfigForScoring.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            return this;
        }

        public Builder modelName(String modelName) {
            cdlModelWorkflowBuilder.modelName(modelName);
            return this;
        }

        public Builder displayName(String displayName) {
            cdlModelWorkflowBuilder.displayName(displayName);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            matchDataCloudWorkflowBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            ratingEngineScoreWorkflowBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            cdlModelWorkflowBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            addStandardAttributes.setSourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public Builder trainingTableName(String trainingTableName) {
            matchDataCloudWorkflowBuilder.matchInputTableName(trainingTableName);
            cdlModelWorkflowBuilder.trainingTableName(trainingTableName);
            return this;
        }

        public Builder targetTableName(String targetTableName) {
            ratingEngineScoreWorkflowBuilder.inputTableName(targetTableName);
            cdlModelWorkflowBuilder.targetTableName(targetTableName);
            return this;
        }

        public Builder userId(String userId) {
            pivotScoreAndEvent.setUserId(userId);
            cdlModelWorkflowBuilder.userId(userId);
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
            cdlModelWorkflowBuilder.addProvenanceProperty(ProvenancePropertyName.TransformationGroupName,
                    transformationGroup.getName());
            return this;
        }

        public Builder sourceModelSummary(ModelSummary modelSummary) {
            cdlModelWorkflowBuilder.sourceModelSummary(modelSummary);
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
            ratingEngineScoreWorkflowBuilder.excludeDataCloudAttrs(exclude);
            cdlModelWorkflowBuilder.excludeDataCloudAttrs(exclude);
            return this;
        }

        public Builder skipDedupStep(boolean skipDedupStep) {
            matchDataCloudWorkflowBuilder.skipDedupStep(skipDedupStep);
            dedupEventTable.setSkipStep(skipDedupStep);
            cdlModelWorkflowBuilder.skipDedupStep(skipDedupStep);
            return this;
        }

        public Builder skipStandardTransform(boolean skipTransform) {
            addStandardAttributes.setSkipStep(skipTransform);
            return this;
        }

        public Builder matchClientDocument(MatchClientDocument matchClientDocument) {
            matchDataCloudWorkflowBuilder.matchClientDocument(matchClientDocument);
            ratingEngineScoreWorkflowBuilder.matchClientDocument(matchClientDocument);
            return this;
        }

        public Builder excludePublicDomain(boolean excludePublicDomains) {
            matchDataCloudWorkflowBuilder.excludePublicDomains(excludePublicDomains);
            ratingEngineScoreWorkflowBuilder.excludeDataCloudAttrs(excludePublicDomains);
            cdlModelWorkflowBuilder.excludePublicDomain(excludePublicDomains);
            return this;
        }

        public Builder setRetainLatticeAccountId(boolean retainLatticeAccountId) {
            matchDataCloudWorkflowBuilder.setRetainLatticeAccountId(retainLatticeAccountId);
            return this;
        }

        public Builder treatPublicDomainAsNormalDomain(boolean publicDomainAsNormalDomain) {
            matchDataCloudWorkflowBuilder.treatPublicDomainAsNormalDomain(publicDomainAsNormalDomain);
            return this;
        }

        public Builder addProvenanceProperty(ProvenancePropertyName propertyName, Object value) {
            cdlModelWorkflowBuilder.addProvenanceProperty(propertyName, value);
            return this;
        }

        public Builder matchType(MatchCommandType matchCommandType) {
            matchDataCloudWorkflowBuilder.matchType(matchCommandType);
            ratingEngineScoreWorkflowBuilder.matchType(matchCommandType);
            return this;
        }

        public Builder matchDestTables(String destTables) {
            matchDataCloudWorkflowBuilder.matchDestTables(destTables);
            ratingEngineScoreWorkflowBuilder.matchDestTables(destTables);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            matchDataCloudWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            ratingEngineScoreWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            cdlModelWorkflowBuilder.dataCloudVersion(dataCloudVersion);
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
            ratingEngineScoreWorkflowBuilder.matchColumnSelection(predefinedColumnSelection, selectionVersion);
            return this;
        }

        public Builder matchDebugEnabled(boolean matchDebugEnabled) {
            // ratingEngineScoreWorkflowBuilder.matchDebugEnabled(matchDebugEnabled);
            return this;
        }

        public Builder matchRequestSource(MatchRequestSource matchRequestSource) {
            matchDataCloudWorkflowBuilder.matchRequestSource(matchRequestSource);
            ratingEngineScoreWorkflowBuilder.matchRequestSource(matchRequestSource);
            return this;
        }

        public Builder userRefinedAttributes(List<Attribute> userRefinedAttributes) {
            resolveAttributes.setUserRefinedAttributes(userRefinedAttributes);
            return this;
        }

        public Builder pivotArtifactPath(String pivotArtifactPath) {
            cdlModelWorkflowBuilder.pivotArtifactPath(pivotArtifactPath);
            return this;
        }

        public Builder dataRules(List<DataRule> dataRules) {
            cdlModelWorkflowBuilder.dataRules(dataRules);
            return this;
        }

        public Builder isDefaultDataRules(boolean isDefaultDataRules) {
            cdlModelWorkflowBuilder.isDefaultDataRules(isDefaultDataRules);
            return this;
        }

        public Builder moduleName(String moduleName) {
            cdlModelWorkflowBuilder.moduleName(moduleName);
            return this;
        }

        public Builder enableV2Profiling(boolean v2ProfilingEnabled) {
            cdlModelWorkflowBuilder.enableV2Profiling(v2ProfilingEnabled);
            return this;
        }

        public Builder notesContent(String notesContent) {
            cdlModelWorkflowBuilder.notesContent(notesContent);
            return this;
        }

        public Builder matchQueue(String queue) {
            matchDataCloudWorkflowBuilder.matchQueue(queue);
            ratingEngineScoreWorkflowBuilder.matchQueue(queue);
            return this;
        }

        public Builder setActivateModelSummaryByDefault(boolean value) {
            cdlModelWorkflowBuilder.setActivateModelSummaryByDefault(value);
            return this;
        }

        public Builder runTimeParams(Map<String, String> runTimeParams) {
            addStandardAttributes.setRuntimeParams(runTimeParams);
            return this;
        }

        public Builder bucketMetadata(List<BucketMetadata> bucketMetadata) {
            ratingEngineScoreWorkflowBuilder.bucketMetadata(bucketMetadata);
            return this;
        }

        public RatingEngineMatchAndModelWorkflowConfiguration build() {
            configuration.setContainerConfiguration("ratingEngineModelAndEmailWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            export.setUsingDisplayName(Boolean.FALSE);

            configuration.add(dedupEventTable);
            configuration.add(matchDataCloudWorkflowBuilder.build());
            configuration.add(cdlModelWorkflowBuilder.build());
            RatingEngineScoreWorkflowConfiguration scoreConf = ratingEngineScoreWorkflowBuilder.build();
            scoreConf.setSkipImport(true);
            configuration.add(scoreConf);
            configuration.add(addStandardAttributes);
            configuration.add(resolveAttributes);
            configuration.add(setConfigForScoring);
            configuration.add(pivotScoreAndEvent);
            configuration.add(export);
            return configuration;
        }

    }
}
