package com.latticeengines.cdl.workflow.listeners;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution.Status;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("processAnalyzeListener")
public class ProcessAnalyzeListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(ProcessAnalyzeListener.class);

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        String customerSpace = job.getTenant().getId();
        String initialDataFeedStatus = job
                .getInputContextValue(WorkflowContextConstants.Inputs.INITIAL_DATAFEED_STATUS);

        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            DataFeedExecution execution = dataFeedProxy.finishExecution(customerSpace, initialDataFeedStatus);
            log.info(String.format("trying to finish running execution %s", execution));
            if (execution.getStatus() != Status.Consolidated) {
                throw new RuntimeException("Can't finish execution");
            }
            // refresh caches
            // CacheService cacheService = CacheServiceBase.getCacheService();
            // cacheService.refreshKeysByPattern(CustomerSpace.parse(customerSpace).getTenantId(),
            // CacheNames.getCdlConsolidateCacheGroup());
            // update segment and rating engine counts
            DataCollection.Version inactiveVersion = dataCollectionProxy.getInactiveVersion(customerSpace);
            log.info("Switch data collection to version " + inactiveVersion);
            dataCollectionProxy.switchVersion(customerSpace, inactiveVersion);
            // update segment and rating engine counts
            SegmentCountUtils.updateEntityCounts(segmentProxy, entityProxy, customerSpace);
            RatingEngineCountUtils.updateRatingEngineCounts(ratingEngineProxy, customerSpace);
        } else if (jobExecution.getStatus() == BatchStatus.FAILED) {
            log.error("workflow failed!");
            DataFeedExecution execution = dataFeedProxy.failExecution(customerSpace, initialDataFeedStatus);
            log.info(String.format("trying to fail running execution %s", execution));
            if (execution.getStatus() != Status.Failed) {
                throw new RuntimeException("Can't fail execution");
            }
        }
    }

}
