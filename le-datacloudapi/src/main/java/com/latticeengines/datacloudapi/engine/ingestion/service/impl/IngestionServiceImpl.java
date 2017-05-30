package com.latticeengines.datacloudapi.engine.ingestion.service.impl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.service.PropDataTenantService;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.core.util.PropDataConstants;
import com.latticeengines.datacloud.etl.SftpUtils;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionNewProgressValidator;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProgressService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProviderService;
import com.latticeengines.datacloudapi.engine.ingestion.service.IngestionService;
import com.latticeengines.domain.exposed.datacloud.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.datacloud.ingestion.ProviderConfiguration;
import com.latticeengines.domain.exposed.datacloud.ingestion.SftpConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion.IngestionType;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("ingestionService")
public class IngestionServiceImpl implements IngestionService {
    private static Log log = LogFactory.getLog(IngestionServiceImpl.class);

    @Autowired
    IngestionEntityMgr ingestionEntityMgr;

    @Autowired
    private IngestionProgressService ingestionProgressService;

    @Autowired
    private IngestionNewProgressValidator ingestionNewProgressValidator;

    @Autowired
    private PropDataTenantService propDataTenantService;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    protected Configuration yarnConfiguration;

    @Resource(name = "ingestionApiProviderService")
    private IngestionProviderService apiProviderService;

    @Resource(name = "ingestionSFTPProviderService")
    private IngestionProviderService sftpProviderService;

    @Autowired
    private EmailService emailService;

    @Value("${propdata.ingestion.notify.email.enabled}")
    private boolean notifyEmailEnabled;

    @Value("${propdata.ingestion.notify.email}")
    private String notifyEmail;

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
    public IngestionProgress ingestInternal(String ingestionName, IngestionRequest ingestionRequest,
            String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
        Ingestion ingestion = getIngestionByName(ingestionName);
        IngestionProgress progress = ingestionProgressService.createDraftProgress(ingestion,
                ingestionRequest.getSubmitter(), ingestionRequest.getFileName());
        if (ingestion.getIngestionType() == IngestionType.SFTP
                && !SftpUtils.ifFileExists((SftpConfiguration) ingestion.getProviderConfiguration(),
                        ingestionRequest.getFileName())) {
            return ingestionProgressService.updateInvalidProgress(progress,
                    ingestionRequest.getFileName() + " does not exist in source SFTP");
        }
        if (ingestionNewProgressValidator.isDuplicateProgress(progress)) {
            return ingestionProgressService.updateInvalidProgress(progress,
                    "There is already a progress ingesting " + progress.getSource() + " to "
                            + progress.getDestination());
        }
        return ingestionProgressService.saveProgress(progress);
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
            ApplicationId appId = ConverterUtils.toApplicationId(progress.getApplicationId());
            try {
                ApplicationReport report = YarnUtils.getApplicationReport(yarnConfiguration, appId);
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
            } catch (YarnException | IOException e) {
                log.error("Failed to track application status for " + progress.getApplicationId()
                        + ". Error: " + e.toString());
                if (e.getMessage().contains("doesn't exist in the timeline store")) {
                    progress = ingestionProgressService.updateProgress(progress)
                            .status(ProgressStatus.FAILED)
                            .errorMessage("Failed to track application status in the scan")
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
            if (ingestionNewProgressValidator.isIngestionTriggered(ingestion)) {
                log.info("Triggered Ingestion: " + ingestion.toString());
                progresses.addAll(createDraftProgresses(ingestion));
            }
        }
        progresses = ingestionNewProgressValidator.cleanupDuplicateProgresses(progresses);
        ingestionProgressService.saveProgresses(progresses);
    }

    private List<IngestionProgress> createDraftProgresses(Ingestion ingestion) {
        List<IngestionProgress> progresses = new ArrayList<>();
        switch (ingestion.getIngestionCriteria()) {
        case ANY_MISSING_FILE:
            List<String> missingFiles = getMissingFiles(ingestion);
            for (String file : missingFiles) {
                IngestionProgress progress = ingestionProgressService.createDraftProgress(ingestion,
                        PropDataConstants.SCAN_SUBMITTER, file);
                progresses.add(progress);
            }
            break;
        case ALL_DATA:
            SimpleDateFormat format = new SimpleDateFormat("_yyyy_MM");
            String fileName = format.format(new Date());
            IngestionProgress progress = ingestionProgressService.createDraftProgress(ingestion,
                    PropDataConstants.SCAN_SUBMITTER, fileName);
            progresses.add(progress);
            break;
        default:
            throw new UnsupportedOperationException(
                    String.format("Ingestion criteria %s is not supported.", ingestion.getIngestionCriteria()));
        }
        return progresses;
    }

    private List<String> getMissingFiles(Ingestion ingestion) {
        switch (ingestion.getIngestionType()) {
        case SFTP:
            return sftpProviderService.getMissingFiles(ingestion);
        case API:
            return apiProviderService.getMissingFiles(ingestion);
        default:
            throw new UnsupportedOperationException(
                    String.format("Ingestion type %s is not supported", ingestion.getIngestionType()));
        }
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
        List<IngestionProgress> submittedProgresses = new ArrayList<IngestionProgress>();
        Boolean serviceTenantBootstrapped = false;
        for (IngestionProgress progress : progresses) {
            try {
                if (!serviceTenantBootstrapped) {
                    propDataTenantService.bootstrapServiceTenant();
                    serviceTenantBootstrapped = true;
                }
                ApplicationId applicationId = submitWorkflow(progress);
                progress = ingestionProgressService.updateSubmittedProgress(progress,
                        applicationId.toString());
                submittedProgresses.add(progress);
                log.info("Submitted workflow for progress [" + progress + "]. ApplicationID = "
                        + applicationId.toString());
            } catch (Exception e) {
                log.error("Failed to submit workflow for progress " + progress, e);
            }
        }
        return submittedProgresses;
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
