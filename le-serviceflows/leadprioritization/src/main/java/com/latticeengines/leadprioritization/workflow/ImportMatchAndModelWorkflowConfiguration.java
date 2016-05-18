package com.latticeengines.leadprioritization.workflow;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.AddStandardAttributesConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.CreatePrematchEventTableReportConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.DedupEventTableConfiguration;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.export.ExportStepConfiguration;
import com.latticeengines.serviceflows.workflow.importdata.ImportStepConfiguration;
import com.latticeengines.serviceflows.workflow.match.MatchStepConfiguration;
import com.latticeengines.serviceflows.workflow.match.ProcessMatchResultConfiguration;
import com.latticeengines.serviceflows.workflow.modeling.ModelStepConfiguration;
import com.latticeengines.serviceflows.workflow.report.BaseReportStepConfiguration;

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
            return this;
        }

        public Builder sourceFileName(String sourceFileName) {
            importData.setSourceFileName(sourceFileName);
            return this;
        }

        public Builder dedupTargetTableName(String targetTableName) {
            dedupEventTable.setTargetTableName(targetTableName);
            match.setInputTableName(targetTableName);
            createEventTableReport.setSourceTableName(targetTableName);
            return this;
        }

        public Builder minPositiveEvents(long minPositiveEvents) {
            createEventTableReport.setMinPositiveEvents(minPositiveEvents);
            return this;
        }

        public Builder minDedupedRows(long minDedupedRows) {
            createEventTableReport.setMinDedupedRows(minDedupedRows);
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
            return this;
        }

        public Builder importReportName(String reportName) {
            registerReport.setReportName(reportName);
            return this;
        }

        public Builder eventTableReportName(String eventTableReportName) {
            createEventTableReport.setReportName(eventTableReportName);
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

        public Builder modelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            model.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
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

        /**
         * You can provide a full column selection object or the name of a predefined selection.
         * When both are present, predefined one will be used.
         * @param customizedColumnSelection
         * @return
         */
        public Builder matchColumnSelection(ColumnSelection customizedColumnSelection) {
            match.setCustomizedColumnSelection(customizedColumnSelection);
            return this;
        }

        /**
         * You can provide a full column selection object or the name of a predefined selection.
         * When both are present, predefined one will be used.
         * If selectionVersion is empty, will use current version.
         * @param predefinedColumnSelection
         * @return
         */
        public Builder matchColumnSelection(ColumnSelection.Predefined predefinedColumnSelection, String selectionVersion) {
            match.setPredefinedColumnSelection(predefinedColumnSelection);
            match.setPredefinedSelectionVersion(selectionVersion);
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
            return this;
        }

        public Builder transformationGroup(TransformationGroup transformationGroup) {
            addStandardAttributes.setTransformationGroup(transformationGroup);
            return this;
        }

        public ImportMatchAndModelWorkflowConfiguration build() {
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

            return configuration;
        }
    }
}
