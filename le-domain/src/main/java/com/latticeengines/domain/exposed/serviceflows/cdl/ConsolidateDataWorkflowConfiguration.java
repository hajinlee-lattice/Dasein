package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ConsolidateAccountDataStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ConsolidateContactDataStepConfiguration;

public class ConsolidateDataWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    private ConsolidateDataWorkflowConfiguration() {
    }

    public static class Builder {
        public ConsolidateDataWorkflowConfiguration configuration = new ConsolidateDataWorkflowConfiguration();
        public ConsolidateAccountDataStepConfiguration consolidateAccountDataConfiguration = new ConsolidateAccountDataStepConfiguration();
        public ConsolidateContactDataStepConfiguration consolidateContactDataConfiguration = new ConsolidateContactDataStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("consolidateDataWorkflow", customerSpace,
                    "consolidateDataWorkflow");
            consolidateAccountDataConfiguration.setCustomerSpace(customerSpace);
            consolidateContactDataConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder accountIdField(String idField) {
            consolidateAccountDataConfiguration.setIdField(idField);
            return this;
        }

        public Builder contactIdField(String idField) {
            consolidateContactDataConfiguration.setIdField(idField);
            return this;
        }

        public Builder matchKeyMap(Map<MatchKey, List<String>> matchKeyMap) {
            consolidateAccountDataConfiguration.setMatchKeyMap(matchKeyMap);
            return this;
        }

        public ConsolidateDataWorkflowConfiguration build() {
            configuration.add(consolidateAccountDataConfiguration);
            configuration.add(consolidateContactDataConfiguration);
            return configuration;
        }
    }
}
