package com.latticeengines.cdl.workflow.steps.process;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CombineStatisticsConfiguration;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("cloneStatistics")
public class CloneStatistics extends BaseWorkflowStep<CombineStatisticsConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CloneStatistics.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    public void execute() {
        log.info("Inside CloneStatistics execute()");
        String customerSpaceStr = configuration.getCustomerSpace().toString();
        DataCollection.Version inactiveVersion = dataCollectionProxy.getInactiveVersion(customerSpaceStr);
        StatisticsContainer statisticsContainer = dataCollectionProxy.getStats(customerSpaceStr);
        statisticsContainer.setVersion(inactiveVersion);
        dataCollectionProxy.upsertStats(customerSpaceStr, statisticsContainer);
    }

}
