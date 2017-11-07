package com.latticeengines.cdl.workflow.listeners;

import java.lang.reflect.Method;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.ProxyUtils;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("profileAndPublishListener")
public class ProfileAndPublishListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(ProfileAndPublishListener.class);

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private EntityProxy entityProxy;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        String initialDataFeedStatus = job.getInputContextValue(WorkflowContextConstants.Inputs.DATAFEED_STATUS);
        String customerSpace = job.getTenant().getId();

        if (jobExecution.getStatus() == BatchStatus.FAILED) {
            log.info(String.format("Workflow failed. Update datafeed status for customer %s with status of %s",
                    customerSpace, initialDataFeedStatus));
            dataFeedProxy.updateDataFeedStatus(customerSpace, initialDataFeedStatus);
        } else if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info(String.format("Workflow completed. Update datafeed status for customer %s with status of %s",
                    customerSpace, Status.Active.getName()));
            dataFeedProxy.finishProfile(customerSpace, Status.Active.getName());
            DataCollection.Version inactiveVersion = dataCollectionProxy.getInactiveVersion(customerSpace);
            log.info("Switch data collection to version " + inactiveVersion);
            dataCollectionProxy.switchVersion(customerSpace, inactiveVersion);
            // update segment counts
            SegmentCountUtils.updateEntityCounts(segmentProxy, entityProxy, customerSpace);
        } else {
            log.warn("Workflow ended in an unknown state.");
        }
    }

    @Component("evictEntityKeyGenerator")
    class EvictEntityKeyGenerator implements KeyGenerator {

        @Override
        public Object generate(Object target, Method method, Object... params) {
            JobExecution jobExecution = (JobExecution) params[0];
            WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
            String customerSpace = job.getTenant().getId();
            return ProxyUtils.shortenCustomerSpace(customerSpace) + "|.*";
        }

    }
}
