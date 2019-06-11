package com.latticeengines.datacloudapi.engine.ingestion.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.service.DataCloudTenantService;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.core.util.PropDataConstants;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProgressService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProviderService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionValidator;
import com.latticeengines.datacloudapi.engine.ingestion.service.IngestionService;
import com.latticeengines.domain.exposed.datacloud.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.datacloud.ingestion.ProviderConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("ingestionService")
public class IngestionServiceImpl implements IngestionService {
    private static Logger log = LoggerFactory.getLogger(IngestionServiceImpl.class);

    @Autowired
    IngestionEntityMgr ingestionEntityMgr;

    @Autowired
    private IngestionProgressService ingestionProgressService;

    @Autowired
    private IngestionValidator ingestionValidator;

    @Autowired
    private DataCloudTenantService dataCloudTenantService;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected YarnClient yarnClient;

    @Resource(name = "ingestionAPIProviderService")
    private IngestionProviderService apiProviderService;

    @Resource(name = "ingestionSFTPProviderService")
    private IngestionProviderService sftpProviderService;

    @Resource(name = "ingestionBWRawProviderService")
    private IngestionProviderService bwRawProviderService;

    public static final int ULTIMATE_RETRIES = 1000;

    @Override
    public List<IngestionProgress> scan(String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
        killFailedProgresses();
        ingestAll();
        return kickoffAll();
    }

    @Override
    public IngestionProgress ingest(String ingestionName, IngestionRequest request,
            String hdfsPod, boolean immediate) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
        Ingestion ingestion = getIngestionByName(ingestionName);
        ingestionValidator.validateIngestionRequest(ingestion, request);
        IngestionProgress progress = ingestionProgressService.createDraftProgress(ingestion, request.getSubmitter(),
                request.getFileName(), request.getSourceVersion());
        if (ingestionValidator.isDuplicateProgress(progress)) {
            return ingestionProgressService.updateInvalidProgress(progress,
                    "There is already a progress ingesting " + progress.getSource() + " to "
                            + progress.getDestination());
        }
        if (immediate) {
            // 1. Progress needs to be saved first before submitting to workflow
            // due to before progress is serialized and passed to workflow, it
            // needs to be
            // populated with PID; otherwise inside workflow, we are not able to
            // update this progress
            // 2. Set status as PROCESSING before submitting to workflow to
            // avoid the corner case that scan is
            // also called at same time and same progress is submitted twice to
            // workflow. If later the progress is failed to be submitted to
            // workflow, status will be updated to FAILED later.
            progress.setStatus(ProgressStatus.PROCESSING);
            progress = ingestionProgressService.saveProgress(progress);
            return submitAll(Collections.singletonList(progress)).get(0);
        } else {
            return ingestionProgressService.saveProgress(progress);
        }
    }

    @Override
    public Ingestion getIngestionByName(String ingestionName) {
        Ingestion ingestion = ingestionEntityMgr.getIngestionByName(ingestionName);
        if (ingestion == null) {
            throw new IllegalArgumentException(String.format("Fail to find ingestion %s", ingestionName));
        }
        return ingestion;
    }

    @Override
    public void killFailedProgresses() {
        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("Status", ProgressStatus.PROCESSING);
        List<IngestionProgress> progresses = ingestionProgressService.getProgressesByField(fields, null);
        for (IngestionProgress progress : progresses) {
            try {
                ApplicationId appId = ApplicationId.fromString(progress.getApplicationId());
                ApplicationReport report = YarnUtils.getApplicationReport(yarnClient, appId);
                if (report == null || report.getYarnApplicationState() == null
                        || report.getYarnApplicationState().equals(YarnApplicationState.FAILED)
                        || report.getYarnApplicationState().equals(YarnApplicationState.KILLED)) {
                    progress = ingestionProgressService.updateProgress(progress)
                            .status(ProgressStatus.FAILED)
                            .errorMessage(
                                    "Found application status to be FAILED or KILLED in the scan")
                            .commit(true);
                    log.info("Killed progress: " + progress.toString());
                }
            } catch (Exception e) {
                log.error("Failed to track application status for {}", progress.getApplicationId(), e);
                if (ExceptionUtils.indexOfThrowable(e, ApplicationNotFoundException.class) > -1) {
                    // ApplicationId no longer exist in RM.
                    // ApplicationNotFoundException is nested exception inside
                    // YarnSystemException
                    progress = ingestionProgressService.updateProgress(progress)
                            .status(ProgressStatus.FAILED)
                            // Should not retry for this case
                            .retries(ULTIMATE_RETRIES)
                            .errorMessage("Application doesn't exist in RM")
                            .commit(true);
                    log.info("Killed progress: " + progress.toString());
                }
            }
        }
    }


    private void ingestAll() {
        List<Ingestion> ingestions = ingestionEntityMgr.findAll();
        List<IngestionProgress> progresses = new ArrayList<>();
        for (Ingestion ingestion : ingestions) {
            if (ingestionValidator.isIngestionTriggered(ingestion)) {
                log.info("Triggered Ingestion: " + ingestion.toString());
                progresses.addAll(createDraftProgresses(ingestion));
            }
        }
        progresses = ingestionValidator.cleanupDuplicateProgresses(progresses);
        ingestionProgressService.saveProgresses(progresses);
    }

    /**
     * Only support ingestion type SFTP, API and BW_RAW in scan
     */
    private List<IngestionProgress> createDraftProgresses(Ingestion ingestion) {
        List<IngestionProgress> progresses = new ArrayList<>();
        switch (ingestion.getIngestionType()) {
        case SFTP:
            List<String> missingFiles = sftpProviderService.getMissingFiles(ingestion);
            for (String file : missingFiles) {
                IngestionProgress progress = ingestionProgressService.createDraftProgress(ingestion,
                        PropDataConstants.SCAN_SUBMITTER, file, null);
                progresses.add(progress);
            }
            break;
        case API:
            missingFiles = apiProviderService.getMissingFiles(ingestion);
            for (String file : missingFiles) {
                IngestionProgress progress = ingestionProgressService.createDraftProgress(ingestion,
                        PropDataConstants.SCAN_SUBMITTER, file, null);
                progresses.add(progress);
            }
            break;
        case BW_RAW:
            missingFiles = bwRawProviderService.getMissingFiles(ingestion);
            for (String file : missingFiles) {
                IngestionProgress progress = ingestionProgressService.createDraftProgress(ingestion,
                        PropDataConstants.SCAN_SUBMITTER, file, null);
                progresses.add(progress);
            }
            break;
        default:
            break;
        }
        return progresses;
    }

    private List<IngestionProgress> kickoffAll() {
        List<IngestionProgress> progresses = new ArrayList<IngestionProgress>();
        List<IngestionProgress> newProgresses = ingestionProgressService
                .getNewIngestionProgresses();
        List<IngestionProgress> retryFailedProgresses = ingestionProgressService
                .getRetryFailedProgresses();
        List<IngestionProgress> processingProgresses = ingestionProgressService
                .getProcessingProgresses();
        Map<String, Integer> processingIngestion = new HashMap<String, Integer>();
        for (IngestionProgress progress : processingProgresses) {
            if (!processingIngestion.containsKey(progress.getIngestionName())) {
                processingIngestion.put(progress.getIngestionName(), 1);
            } else {
                processingIngestion.put(progress.getIngestionName(),
                        processingIngestion.get(progress.getIngestionName()) + 1);
            }
        }
        for (IngestionProgress progress : retryFailedProgresses) {
            if (!processingIngestion.containsKey(progress.getIngestionName())) {
                progresses.add(progress);
                processingIngestion.put(progress.getIngestionName(), 1);
            } else if (progress.getIngestion().getProviderConfiguration().getConcurrentNum() > processingIngestion
                    .get(progress.getIngestionName())) {
                progresses.add(progress);
                processingIngestion.put(progress.getIngestionName(),
                        processingIngestion.get(progress.getIngestionName()) + 1);
            }
        }
        for (IngestionProgress progress : newProgresses) {
            if (!processingIngestion.containsKey(progress.getIngestionName())) {
                progresses.add(progress);
                processingIngestion.put(progress.getIngestionName(), 1);
            } else if (progress.getIngestion().getProviderConfiguration().getConcurrentNum() > processingIngestion
                    .get(progress.getIngestionName())) {
                progresses.add(progress);
                processingIngestion.put(progress.getIngestionName(),
                        processingIngestion.get(progress.getIngestionName()) + 1);
            }
        }
        return submitAll(progresses);
    }

    private List<IngestionProgress> submitAll(List<IngestionProgress> progresses) {
        List<IngestionProgress> submittedProgresses = new ArrayList<>();
        Boolean serviceTenantBootstrapped = false;
        for (IngestionProgress progress : progresses) {
            try {
                if (!serviceTenantBootstrapped) {
                    dataCloudTenantService.bootstrapServiceTenant();
                    serviceTenantBootstrapped = true;
                }
                progress = submit(progress);
                submittedProgresses.add(progress);
            } catch (Exception e) {
                log.error("Failed to submit workflow for progress " + progress, e);
                progress = ingestionProgressService.updateProgress(progress) //
                        .status(ProgressStatus.FAILED) //
                        .errorMessage(e.getMessage().substring(0, Math.min(1000, e.getMessage().length()))) //
                        .commit(true);
            }
        }
        return submittedProgresses;
    }

    private IngestionProgress submit(IngestionProgress progress) {
        ApplicationId applicationId = submitWorkflow(progress);
        progress = ingestionProgressService.updateSubmittedProgress(progress,
                applicationId.toString());
        log.info("Submitted workflow for progress [" + progress + "]. ApplicationID = "
                + applicationId.toString());
        return progress;
    }

    private ApplicationId submitWorkflow(IngestionProgress progress) {
        Ingestion ingestion = progress.getIngestion();
        ProviderConfiguration providerConfiguration = ingestion.getProviderConfiguration();
        return new IngestionWorkflowSubmitter() //
                .workflowProxy(workflowProxy) //
                .ingestionProgress(progress) //
                .ingestion(ingestion) //
                .providerConfiguration(providerConfiguration) //
                .submit();
    }
}
