package com.latticeengines.cdl.workflow.listeners;

import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher.CDLConsolidate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.camille.exposed.watchers.NodeWatcher;
import com.latticeengines.domain.exposed.cache.CacheNames;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution.Status;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("datafeedExecutionListener")
public class DataFeedExecutionListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(DataFeedExecutionListener.class);

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private CacheService cacheService;

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
            cacheService.dropKeysByPattern(CacheNames.EntityCache.name(), String.format("*%s*", customerSpace));
            NodeWatcher.updateWatchedData(CDLConsolidate.name(), customerSpace);
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
