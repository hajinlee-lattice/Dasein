package com.latticeengines.cdl.workflow.steps;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedProfile;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CalculateStatsStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.SortContactStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CalculatePurchaseHistoryConfiguration;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("startProfile")
public class StartProfile extends BaseWorkflowStep<CalculateStatsStepConfiguration> {

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Override
    public void execute() {
        DataFeedProfile profile = dataFeedProxy.updateProfileWorkflowId(configuration.getCustomerSpace().toString(),
                jobId);
        if (profile == null) {
            throw new RuntimeException("profile is null!!");
        } else if (profile.getWorkflowId().longValue() != jobId.longValue()) {
            throw new RuntimeException(
                    String.format("current active profile has a workflow id %s, which is different from %s ",
                            profile.getWorkflowId(), jobId));
        }

        Table contactMasterTable = dataCollectionProxy.getTable(configuration.getCustomerSpace().toString(),
                TableRoleInCollection.ConsolidatedContact);
        if (contactMasterTable == null) {
            log.info("Cannot find the contact master table in default collection");
            SortContactStepConfiguration sortContactStepConfig = getConfigurationFromJobParameters(
                    SortContactStepConfiguration.class);
            sortContactStepConfig.setSkipStep(true);
            putObjectInContext(SortContactStepConfiguration.class.getName(), sortContactStepConfig);
        }

        Table transactionTable = dataCollectionProxy.getTable(configuration.getCustomerSpace().toString(),
                                                              TableRoleInCollection.AggregatedTransaction);
        Table productTable = dataCollectionProxy.getTable(configuration.getCustomerSpace().toString(),
                                                          TableRoleInCollection.ConsolidatedProduct);
        if ((transactionTable == null) || (productTable == null)) {
            log.info("Skip profile purchase history since either product or transaction table does not exist");
            CalculatePurchaseHistoryConfiguration calculatePurchaseHistoryConfig = getConfigurationFromJobParameters(
                    CalculatePurchaseHistoryConfiguration.class);
            calculatePurchaseHistoryConfig.setSkipStep(true);
            putObjectInContext(CalculatePurchaseHistoryConfiguration.class.getName(), calculatePurchaseHistoryConfig);
        }
    }
}
