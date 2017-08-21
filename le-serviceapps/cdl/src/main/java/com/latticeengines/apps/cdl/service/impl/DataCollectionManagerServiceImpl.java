package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DataCollectionManagerService;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedProfile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("dataCollectionManagerService")
public class DataCollectionManagerServiceImpl implements DataCollectionManagerService {

    private static final Logger log = LoggerFactory.getLogger(DataCollectionManagerServiceImpl.class);

    private final DataFeedProxy dataFeedProxy;

    private final DataCollectionProxy dataCollectionProxy;

    private final WorkflowProxy workflowProxy;

    @Inject
    public DataCollectionManagerServiceImpl(DataFeedProxy dataFeedProxy, DataCollectionProxy dataCollectionProxy,
            WorkflowProxy workflowProxy) {
        this.dataFeedProxy = dataFeedProxy;
        this.dataCollectionProxy = dataCollectionProxy;
        this.workflowProxy = workflowProxy;
    }

    @Override
    public boolean resetAll(String customerSpaceStr) {
        DataFeed df = dataFeedProxy.getDataFeed(customerSpaceStr);

        DataFeed.Status status = df.getStatus();
        if ((status == DataFeed.Status.Deleting) || (status == DataFeed.Status.Initing)) {
            return true;
        }

        if ((status == DataFeed.Status.Consolidating) || (status == DataFeed.Status.Profiling)) {
            quiesceDataFeed(customerSpaceStr, df);
        }

        dataFeedProxy.updateDataFeedStatus(customerSpaceStr, DataFeed.Status.Initing.getName());

        resetBatchStore(customerSpaceStr, BusinessEntity.Contact);
        resetBatchStore(customerSpaceStr, BusinessEntity.Account);

        resetImport(customerSpaceStr);

        return true;

    }

    @Override
    public boolean resetEntity(String customerSpaceStr, BusinessEntity entity) {
        DataFeed df = dataFeedProxy.getDataFeed(customerSpaceStr);
        DataFeed.Status status = df.getStatus();
        if ((status == DataFeed.Status.Deleting) || (status == DataFeed.Status.Initing)
                || (status == DataFeed.Status.InitialLoaded)) {
            return true;
        } else if ((df.getStatus() == DataFeed.Status.Profiling) || (df.getStatus() == DataFeed.Status.Consolidating)) {
            return false;
        }
        resetBatchStore(customerSpaceStr, entity);
        dataFeedProxy.updateDataFeedStatus(customerSpaceStr, DataFeed.Status.InitialLoaded.getName());
        return true;
    }

    private void stopWorkflow(Long workflowId) {
        if (workflowId == null) {
            return;
        }
        try {
            Job job = workflowProxy.getWorkflowExecution(workflowId.toString());
            if ((job != null) && (job.isRunning())) {
                workflowProxy.stopWorkflow(workflowId.toString());
            }
        } catch (Exception e) {
            log.error("Failed to stop workflow " + workflowId, e);
        }
    } 

    private void quiesceDataFeed(String customerSpaceStr, DataFeed df) {
        DataFeedExecution exec = df.getActiveExecution();
        if (exec != null) {
            stopWorkflow(exec.getWorkflowId());
            dataFeedProxy.finishExecution(customerSpaceStr, DataFeed.Status.Active.getName());
        }

        DataFeedProfile profile = df.getActiveProfile();
        if (profile != null) {
            stopWorkflow(profile.getWorkflowId());
        }
    }


    private void resetImport(String customerSpaceStr) {
        dataFeedProxy.resetImport(customerSpaceStr);
    }

    private void resetBatchStore(String customerSpaceStr, BusinessEntity entity) {
        dataCollectionProxy.resetTable(customerSpaceStr, entity.getBatchStore());
    }
}
