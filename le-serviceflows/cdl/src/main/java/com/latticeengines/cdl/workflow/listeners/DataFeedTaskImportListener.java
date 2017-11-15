package com.latticeengines.cdl.workflow.listeners;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.ImportStatus;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.pls.VdbLoadTableStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.eai.EaiJobDetailProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.remote.exposed.service.DataLoaderService;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("dataFeedTaskImportListener")
public class DataFeedTaskImportListener extends LEJobListener {

    private final static Logger log = LoggerFactory.getLogger(DataFeedTaskImportListener.class);

    @Autowired
    private EaiJobDetailProxy eaiJobDetailProxy;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Autowired
    private DataLoaderService dataLoaderService;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {

    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        String importJobIdentifier = job.getInputContextValue(WorkflowContextConstants.Inputs.DATAFEEDTASK_IMPORT_IDENTIFIER);
        String customerSpace = job.getTenant().getId();
        EaiImportJobDetail eaiImportJobDetail = eaiJobDetailProxy.getImportJobDetailByCollectionIdentifier(importJobIdentifier);
        if (eaiImportJobDetail == null) {
            log.warn(String.format("Cannot find the job detail for %s", importJobIdentifier));
            return;
        }

        VdbLoadTableStatus vdbLoadTableStatus = null;
        String statusUrl = eaiImportJobDetail.getReportURL();
        String queryHandle = eaiImportJobDetail.getQueryHandle();
        if(statusUrl != null && !statusUrl.isEmpty() && queryHandle != null && !queryHandle.isEmpty()) {
            vdbLoadTableStatus = new VdbLoadTableStatus();
            vdbLoadTableStatus.setVdbQueryHandle(queryHandle);
        }

        if (jobExecution.getStatus().isUnsuccessful()) {
            updateEaiImportJobDetail(eaiImportJobDetail, ImportStatus.FAILED);
        } else if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            try {
                String templateName = eaiImportJobDetail.getTemplateName();
                List<String> pathList = eaiImportJobDetail.getPathDetail();
                List<String> processedRecordsList = eaiImportJobDetail.getPRDetail();
                if (pathList == null || processedRecordsList == null || pathList.size() != processedRecordsList.size()) {
                    log.error("Error in extract info, skip register extract.");
                    updateEaiImportJobDetail(eaiImportJobDetail, ImportStatus.FAILED);

                    if(vdbLoadTableStatus!=null) {
                        vdbLoadTableStatus.setJobStatus("Failed");
                        vdbLoadTableStatus.setMessage("Error in extract info, skip register extract.");
                        dataLoaderService.reportGetDataStatus(statusUrl, vdbLoadTableStatus);
                    }

                    return;
                }
                List<Extract> extracts = new ArrayList<>();
                for (int i = 0; i < pathList.size(); i++) {
                    log.info(String.format("Extract %s have %s records.", pathList.get(i), processedRecordsList.get(i)));
                    long records = Long.parseLong(processedRecordsList.get(i));
                    extracts.add(createExtract(pathList.get(i), records));
                }
                if (extracts.size() == 1) {
                    log.info(String.format("Register single extract: %s", extracts.get(0).getName()));
                    dataFeedProxy.registerExtract(customerSpace, importJobIdentifier, templateName, extracts.get(0));
                } else {
                    log.info(String.format("Register %d extracts.", extracts.size()));
                    dataFeedProxy.registerExtracts(customerSpace, importJobIdentifier, templateName, extracts);
                }
                updateEaiImportJobDetail(eaiImportJobDetail, ImportStatus.SUCCESS);
                updateDataFeedStatus(customerSpace);

                if(vdbLoadTableStatus!=null) {
                    vdbLoadTableStatus.setJobStatus("Succeed");
                    vdbLoadTableStatus.setMessage("Load table complete!");
                    dataLoaderService.reportGetDataStatus(statusUrl, vdbLoadTableStatus);
                }
            } catch (Exception e) {
                updateEaiImportJobDetail(eaiImportJobDetail, ImportStatus.FAILED);

                if(vdbLoadTableStatus!=null) {
                    vdbLoadTableStatus.setJobStatus("Failed");
                    vdbLoadTableStatus.setMessage(String.format("Load table failed with exception: %s", e.toString()));
                    dataLoaderService.reportGetDataStatus(statusUrl, vdbLoadTableStatus);
                }
            }

        } else {
            log.error(String.format("DataFeedTask import job ends in unknown status: %s",
                    jobExecution.getStatus().name()));
            updateEaiImportJobDetail(eaiImportJobDetail, ImportStatus.FAILED);

            if(vdbLoadTableStatus!=null) {
                vdbLoadTableStatus.setJobStatus("Failed");
                vdbLoadTableStatus.setMessage(String.format("DataFeedTask import job ends in unknown status: %s",
                        jobExecution.getStatus().name()));
                dataLoaderService.reportGetDataStatus(statusUrl, vdbLoadTableStatus);
            }
        }
    }

    private void updateDataFeedStatus(String customerSpace) {
        DataFeed dataFeed = dataFeedProxy.getDataFeed(customerSpace);
        if (dataFeed.getStatus() == DataFeed.Status.Initialized) {
            dataFeedProxy.updateDataFeedStatus(customerSpace, DataFeed.Status.InitialLoaded.getName());
        }
    }

    private void updateEaiImportJobDetail(EaiImportJobDetail eaiImportJobDetail, ImportStatus importStatus) {
        eaiImportJobDetail.setStatus(importStatus);
        eaiJobDetailProxy.updateImportJobDetail(eaiImportJobDetail);
    }


    private Extract createExtract(String path, long processedRecords) {
        Extract e = new Extract();
        e.setName(StringUtils.substringAfterLast(path, "/"));
        e.setPath(PathUtils.stripoutProtocol(path));
        e.setProcessedRecords(processedRecords);
        String dateTime = StringUtils.substringBetween(path, "/Extracts/", "/");
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        try {
            e.setExtractionTimestamp(f.parse(dateTime).getTime());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return e;
    }
}
