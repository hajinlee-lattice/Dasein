package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status.RUNNING_STATUS;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataFeedEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.apps.cdl.service.DataFeedExecutionCleanupService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("dataFeedExecutionCleanupService")
public class DataFeedExecutionCleanupServiceImpl implements DataFeedExecutionCleanupService {

    private static final Logger log = LoggerFactory.getLogger(DataFeedExecutionCleanupServiceImpl.class);

    @Inject
    private DataFeedExecutionEntityMgr dataFeedExecutionEntityMgr;

    @Inject
    private DataFeedEntityMgr dataFeedEntityMgr;

    @Inject
    private WorkflowProxy workflowProxy;

    @Value("${cdl.datafeed.execution.stuck.min:30}")
    private int stuckMin;

    @Override
    public boolean removeStuckExecution(String jobArguments) {
        List<SimpleDataFeed> allSimpleDataFeeds = dataFeedEntityMgr.getAllSimpleDataFeeds();
        if (CollectionUtils.isEmpty(allSimpleDataFeeds)) {
            return true;
        }
        List<SimpleDataFeed> runningFeeds = allSimpleDataFeeds.stream()
                .filter(simpleDataFeed -> RUNNING_STATUS.contains(simpleDataFeed.getStatus()))
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(runningFeeds)) {
            log.info("There is no datafeed in running status.");
            return true;
        }

        for (SimpleDataFeed simpleDataFeed : runningFeeds) {
            MultiTenantContext.setTenant(simpleDataFeed.getTenant());
            try {
                DataFeed dataFeed = dataFeedEntityMgr.findDefaultFeed();
                if (dataFeed == null) {
                    log.warn("Cannot find default data feed for tenant: " + simpleDataFeed.getTenant().getId());
                    continue;
                }
                dataFeed = dataFeedEntityMgr.findByName(dataFeed.getName());
                DataFeedExecution dataFeedExecution = dataFeed.getActiveExecution();
                if (dataFeedExecution == null) {
                    log.info("Cannot find datafeed Execution for data feed: " + dataFeed.getName());
                    continue;
                }
                // 1. cleanup stuck execution
                if (dataFeedExecution.getWorkflowId() == null
                        && DataFeedExecution.Status.Started.equals(dataFeedExecution.getStatus())) {
                    Date lastUpdated = dataFeedExecution.getUpdated() == null ? dataFeedExecution.getCreated() :
                            dataFeedExecution.getUpdated();
                    if (System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(stuckMin) > lastUpdated.getTime()) {
                        log.info("Reset datafeed_execution status from Started to Failed");
                        dataFeedExecution.setStatus(DataFeedExecution.Status.Failed);
                        dataFeedExecutionEntityMgr.update(dataFeedExecution);

                        log.info(String.format("Reset datafeed status from %s to Active", dataFeed.getStatus().name()));
                        dataFeed.setStatus(DataFeed.Status.Active);
                        dataFeedEntityMgr.update(dataFeed);
                    }
                }
                // 2. cleanup laze update ones
                if (dataFeedExecution.getWorkflowId() != null) {
                    Job job = workflowProxy.getWorkflowExecution(String.valueOf(dataFeedExecution.getWorkflowId()),
                            MultiTenantContext.getCustomerSpace().toString());
                    if (job != null && job.getJobStatus().isTerminated()) {
                        DataFeedExecution.Status dfeStatus = DataFeedExecution.Status.Failed;
                        if (JobStatus.COMPLETED.equals(job.getJobStatus())) {
                            dfeStatus = DataFeedExecution.Status.Completed;
                        }
                        log.info("Reset datafeed_execution status from Started to " + dfeStatus.name());
                        dataFeedExecution.setStatus(dfeStatus);
                        dataFeedExecutionEntityMgr.update(dataFeedExecution);

                        log.info(String.format("Reset datafeed %s status from %s to Active (Lazy update)",
                                dataFeed.getName(), dataFeed.getStatus().name()));
                        dataFeed.setStatus(DataFeed.Status.Active);
                        dataFeedEntityMgr.update(dataFeed);
                    }
                }
            } catch (Exception e) {
                log.error(String.format("Cannot check datafeed status for tenant %s, exception: %s",
                        simpleDataFeed.getTenant().getId(), e.getMessage()));
                continue;
            }
        }
        return true;
    }
}
