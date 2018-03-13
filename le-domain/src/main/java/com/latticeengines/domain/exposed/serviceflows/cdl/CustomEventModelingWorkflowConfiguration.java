package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;
import java.util.Map;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.modeling.ModelingType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.serviceflows.core.steps.AddStandardAttributesConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.BaseReportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.ModelDataValidationWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.ModelWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.DedupEventTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.RTSBulkScoreWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.PivotScoreAndEventConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

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

        private PivotScoreAndEventConfiguration pivotScoreAndEvent = new PivotScoreAndEventConfiguration();
        private ExportStepConfiguration export = new ExportStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            customEventMatchWorkflowConfigurationBuilder.customer(customerSpace);
            importData.setCustomerSpace(customerSpace);
            registerReport.setCustomerSpace(customerSpace);
            modelDataValidationWorkflow.customer(customerSpace);
            dedupEventTable.setCustomerSpace(customerSpace);
            addStandardAttributes.setCustomerSpace(customerSpace);
            export.setCustomerSpace(customerSpace);
            modelWorkflowBuilder.customer(customerSpace);
            prepareConfigForScoringBuilder.customer(customerSpace);
            rtsBulkScoreWorkflowBuilder.customer(customerSpace);
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
            modelWorkflowBuilder.microServiceHostPort(microServiceHostPort);

            rtsBulkScoreWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            prepareConfigForScoringBuilder.microServiceHostPort(microServiceHostPort);
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
            rtsBulkScoreWorkflowBuilder.matchClientDocument(matchClientDocument);
            return this;
        }

        public Builder matchInputTableName(String tableName) {
            customEventMatchWorkflowConfigurationBuilder.matchInputTableName(tableName);
            return this;
        }

        public Builder modelingType(ModelingType modelingType) {
            customEventMatchWorkflowConfigurationBuilder.modelingType(modelingType);
            prepareConfigForScoringBuilder.modelingType(modelingType);
            return this;
        }

        public Builder metadataSegmentExport(MetadataSegmentExport metadataSegmentExport) {
            prepareConfigForScoringBuilder.metadataSegmentExport(metadataSegmentExport);
            return this;
        }

        public Builder matchRequestSource(MatchRequestSource matchRequestSource) {
            customEventMatchWorkflowConfigurationBuilder.matchRequestSource(matchRequestSource);
            return this;
        }

        public Builder matchQueue(String queue) {
            customEventMatchWorkflowConfigurationBuilder.matchQueue(queue);
            return this;
        }

        public Builder matchColumnSelection(Predefined predefinedColumnSelection, String selectionVersion) {
            customEventMatchWorkflowConfigurationBuilder.matchColumnSelection(predefinedColumnSelection,
                    selectionVersion);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            customEventMatchWorkflowConfigurationBuilder.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchType(MatchCommandType matchCommandType) {
            customEventMatchWorkflowConfigurationBuilder.matchType(matchCommandType);
            return this;
        }

        public Builder setRetainLatticeAccountId(boolean retainLatticeAccountId) {
            customEventMatchWorkflowConfigurationBuilder.setRetainLatticeAccountId(retainLatticeAccountId);
            return this;
        }

        public Builder matchDestTables(String destTables) {
            customEventMatchWorkflowConfigurationBuilder.matchDestTables(destTables);
            return this;
        }

        public Builder excludePublicDomains(boolean excludePublicDomains) {
            customEventMatchWorkflowConfigurationBuilder.excludePublicDomains(excludePublicDomains);
            return this;
        }

        public Builder excludeDataCloudAttrs(boolean exclude) {
            customEventMatchWorkflowConfigurationBuilder.excludeDataCloudAttrs(exclude);
            return this;
        }

        public Builder skipDedupStep(boolean skipDedupStep) {
            customEventMatchWorkflowConfigurationBuilder.skipDedupStep(skipDedupStep);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            customEventMatchWorkflowConfigurationBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public CustomEventModelingWorkflowConfiguration build() {
            configuration.setContainerConfiguration("customEventModelingWorkflowConfiguration",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(customEventMatchWorkflowConfigurationBuilder.build());
            return configuration;
        }
    }
}
