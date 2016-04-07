package com.latticeengines.leadprioritization.workflow;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.AddStandardAttributesConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.DedupEventTableConfiguration;
import com.latticeengines.serviceflows.workflow.importdata.ImportStepConfiguration;
import com.latticeengines.serviceflows.workflow.match.MatchStepConfiguration;
import com.latticeengines.serviceflows.workflow.modeling.ModelStepConfiguration;
import com.latticeengines.serviceflows.workflow.report.BaseReportStepConfiguration;

public class ImportMatchAndModelWorkflowConfiguration extends WorkflowConfiguration {
    public static class Builder {
        private ImportMatchAndModelWorkflowConfiguration configuration = new ImportMatchAndModelWorkflowConfiguration();
        private ImportStepConfiguration importData = new ImportStepConfiguration();
        private BaseReportStepConfiguration registerReport = new BaseReportStepConfiguration();
        private DedupEventTableConfiguration dedupEventTable = new DedupEventTableConfiguration();
        private ModelStepConfiguration model = new ModelStepConfiguration();
        private MatchStepConfiguration match = new MatchStepConfiguration();
        private AddStandardAttributesConfiguration addStandardAttributes = new AddStandardAttributesConfiguration();

        public Builder microServiceHostPort(String microServiceHostPort) {
            importData.setMicroServiceHostPort(microServiceHostPort);
            registerReport.setMicroServiceHostPort(microServiceHostPort);
            dedupEventTable.setMicroServiceHostPort(microServiceHostPort);
            model.setMicroServiceHostPort(microServiceHostPort);
            match.setMicroServiceHostPort(microServiceHostPort);
            addStandardAttributes.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("importMatchAndModelWorkflow", customerSpace,
                    "ImportMatchAndModelWorkflow");
            importData.setCustomerSpace(customerSpace);
            registerReport.setCustomerSpace(customerSpace);
            dedupEventTable.setCustomerSpace(customerSpace);
            model.setCustomerSpace(customerSpace);
            match.setCustomerSpace(customerSpace);
            addStandardAttributes.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder sourceFileName(String sourceFileName) {
            importData.setSourceFileName(sourceFileName);
            return this;
        }

        public Builder dedupTargetTableName(String targetTableName) {
            dedupEventTable.setTargetTableName(targetTableName);
            match.setInputTableName(targetTableName);
            return this;
        }

        public Builder sourceType(SourceType sourceType) {
            importData.setSourceType(sourceType);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            importData.setInternalResourceHostPort(internalResourceHostPort);
            registerReport.setInternalResourceHostPort(internalResourceHostPort);
            dedupEventTable.setInternalResourceHostPort(internalResourceHostPort);
            model.setInternalResourceHostPort(internalResourceHostPort);
            match.setInternalResourceHostPort(internalResourceHostPort);
            addStandardAttributes.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder reportName(String reportName) {
            registerReport.setReportName(reportName);
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

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public ImportMatchAndModelWorkflowConfiguration build() {
            configuration.add(importData);
            configuration.add(registerReport);
            configuration.add(dedupEventTable);
            configuration.add(match);
            configuration.add(model);
            configuration.add(addStandardAttributes);

            return configuration;
        }
    }
}
