package com.latticeengines.cdl.workflow.listeners;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.domain.exposed.cache.CacheNames;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution.Status;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.cdl.CDLJobProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("datafeedExecutionListener")
public class DataFeedExecutionListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(DataFeedExecutionListener.class);

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private CDLJobProxy cdlJobProxy;

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
        String draining = job.getInputContextValue(WorkflowContextConstants.Inputs.DRAINING);
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            DataFeedExecution execution = dataFeedProxy.finishExecution(customerSpace, initialDataFeedStatus);
            log.info(String.format("trying to finish running execution %s", execution));
            if (execution.getStatus() != Status.Consolidated) {
                throw new RuntimeException("Can't finish execution");
            }
            // refresh caches
            CacheService cacheService = CacheServiceBase.getCacheService();
            cacheService.refreshKeysByPattern(CustomerSpace.parse(customerSpace).getTenantId(),
                    CacheNames.getCdlConsolidateCacheGroup());
            // update segment and rating engine counts
            SegmentCountUtils.updateEntityCounts(segmentProxy, entityProxy, customerSpace);
            RatingEngineCountUtils.updateRatingEngineCounts(ratingEngineProxy, customerSpace);
            log.info(String.format("trying to run profile after execution %s", execution));
            if (Boolean.valueOf(draining)) {
                try {
                    cdlJobProxy.createProfileJob(customerSpace);
                } catch (Exception e) {
                    log.error("profile error: " + e.getMessage());
                }
            }
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
