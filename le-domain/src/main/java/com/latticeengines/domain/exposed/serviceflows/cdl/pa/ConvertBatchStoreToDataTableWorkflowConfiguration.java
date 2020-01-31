package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.activity.ActivityImport;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch.ConvertAccountWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch.ConvertContactWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch.ConvertTransactionWorkflowConfiguration;

public class ConvertBatchStoreToDataTableWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private ConvertBatchStoreToDataTableWorkflowConfiguration configuration =
                new ConvertBatchStoreToDataTableWorkflowConfiguration();
        private ConvertAccountWorkflowConfiguration.Builder convertAccountWorkflowBuilder =
                new ConvertAccountWorkflowConfiguration.Builder();
        private ConvertContactWorkflowConfiguration.Builder convertContactWorkflowBuilder =
                new ConvertContactWorkflowConfiguration.Builder();
        private ConvertTransactionWorkflowConfiguration.Builder convertTransactionWorkflowBuilder =
                new ConvertTransactionWorkflowConfiguration.Builder();
        private ProcessActivityStreamStepConfiguration processActivityStreamStepConfiguration =
                new ProcessActivityStreamStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            convertAccountWorkflowBuilder.customer(customerSpace);
            convertContactWorkflowBuilder.customer(customerSpace);
            convertTransactionWorkflowBuilder.customer(customerSpace);
            processActivityStreamStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            convertAccountWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            convertContactWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            convertTransactionWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processActivityStreamStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder entityMatchEnabled(boolean entityMatchEnabled) {
            processActivityStreamStepConfiguration.setSkipStep(!entityMatchEnabled);
            processActivityStreamStepConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            return this;
        }

        /**
         *
         * @param entityList those entity haven't batchStore, skip this step
         */
        public Builder setSkipStep(Set<BusinessEntity> entityList) {
            convertAccountWorkflowBuilder.setSkipStep(!CollectionUtils.isEmpty(entityList) && entityList.contains(BusinessEntity.Account));
            convertContactWorkflowBuilder.setSkipStep(!CollectionUtils.isEmpty(entityList) && entityList.contains(BusinessEntity.Contact));
            convertTransactionWorkflowBuilder.setSkipStep(!CollectionUtils.isEmpty(entityList) && entityList.contains(BusinessEntity.Transaction));
            return this;
        }

        public Builder setConvertServiceConfig(HashMap<TableRoleInCollection, Table> batchStoresToConvert) {
            convertAccountWorkflowBuilder.setConvertServiceConfig(batchStoresToConvert);
            convertContactWorkflowBuilder.setConvertServiceConfig(batchStoresToConvert);
            convertTransactionWorkflowBuilder.setConvertServiceConfig(batchStoresToConvert);
            return this;
        }

        public Builder entityMatchGAOnly(boolean entityMatchGAOnly) {
            processActivityStreamStepConfiguration.setEntityMatchGAOnly(entityMatchGAOnly);
            return this;
        }

        public Builder activeRawStreamTables(Map<String, String> rawStreamTables) {
            processActivityStreamStepConfiguration.setActiveRawStreamTables(rawStreamTables);
            return this;
        }

        public Builder activityStreams(Map<String, AtlasStream> activityStreams) {
            processActivityStreamStepConfiguration.setActivityStreamMap(activityStreams);
            return this;
        }

        public Builder activityMetricsGroups(Map<String, ActivityMetricsGroup> groups) {
            processActivityStreamStepConfiguration.setActivityMetricsGroupMap(groups);
            return this;
        }

        public Builder activityStreamImports(Map<String, List<ActivityImport>> activityStreamImports) {
            processActivityStreamStepConfiguration.setStreamImports(activityStreamImports);
            return this;
        }

        public Builder setReplaceMode(boolean isReplaceMode) {
            processActivityStreamStepConfiguration.setReplaceMode(isReplaceMode);
            return this;
        }

        public Builder setRematchMode(boolean isRematchMode) {
            processActivityStreamStepConfiguration.setRematchMode(isRematchMode);
            return this;
        }

        public ConvertBatchStoreToDataTableWorkflowConfiguration build() {
            configuration.setContainerConfiguration("convertBatchStoreToDataTableWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            setDiscardFields();
            configuration.add(convertAccountWorkflowBuilder.build());
            configuration.add(convertContactWorkflowBuilder.build());
            configuration.add(convertTransactionWorkflowBuilder.build());
            configuration.add(processActivityStreamStepConfiguration);
            return configuration;
        }

        private void setDiscardFields() {
            List<String> discardFields = new ArrayList<>();
            discardFields.add(InterfaceName.TransactionDate.name());
            discardFields.add(InterfaceName.TransactionDayPeriod.name());
            convertTransactionWorkflowBuilder.withDiscardFields(discardFields);
        }
    }
}
