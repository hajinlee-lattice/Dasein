package com.latticeengines.datacloudapi.engine.publication.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.service.DataCloudTenantService;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.core.util.PropDataConstants;
import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationEntityMgr;
import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.datacloud.etl.publication.service.PublicationProgressService;
import com.latticeengines.datacloud.etl.publication.service.PublicationProgressUpdater;
import com.latticeengines.datacloud.etl.service.DataCloudEngineService;
import com.latticeengines.datacloudapi.engine.publication.service.PublicationService;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngine;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngineStage;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationRequest;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("publicationService")
public class PublicationServiceImpl implements PublicationService, DataCloudEngineService {

    private static final Logger log = LoggerFactory.getLogger(PublicationServiceImpl.class);

    private static final Long NEW_JOB_TIMEOUT = TimeUnit.MINUTES.toMillis(10);
    private static final Long RUNNING_JOB_TIMEOUT = TimeUnit.HOURS.toMillis(48);

    @Autowired
    private PublicationEntityMgr publicationEntityMgr;

    @Autowired
    private PublicationProgressEntityMgr progressEntityMgr;

    @Autowired
    private PublicationProgressService publicationProgressService;

    @Autowired
    private DataCloudTenantService dataCloudTenantService;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private YarnClient yarnClient;

    @Override
    public List<PublicationProgress> scan() {
        checkFailedProgresses();
        killHangingJobs();
        publishAll();
        return scanForNewWorkFlow();
    }

    @Override
    public PublicationProgress kickoff(String publicationName, PublicationRequest request) {
        Publication publication = publicationEntityMgr.findByPublicationName(publicationName);
        PublicationProgress progress = publicationProgressService.publishVersion(publication,
                request.getSourceVersion(), request.getSubmitter());
        if (progress == null) {
            log.info("There is already a progress for version " + request.getSourceVersion());
        }
        return progress;
    }

    @Override
    public AppSubmission publish(String publicationName, PublicationRequest request) {
        Publication publication = publicationEntityMgr.findByPublicationName(publicationName);
        if (publication == null) {
            throw new IllegalArgumentException("Cannot find publication named " + publicationName);
        }
        PublicationProgress progress = publicationProgressService.publishVersion(publication, request.getDestination(),
                request.getSourceVersion(), request.getSubmitter());
        ApplicationId appId = submitWorkflow(progress);
        publicationProgressService.update(progress).applicationId(appId).commit();
        return new AppSubmission(appId);
    }

    private void publishAll() {
        for (Publication publication : publicationEntityMgr.findAll()) {
            if (publication.isSchedularEnabled()) {
                try {
                    publicationProgressService.kickoffNewProgress(publication, PropDataConstants.SCAN_SUBMITTER);
                } catch (Exception e) {
                    log.error("Failed to trigger publication " + publication.getPublicationName(), e);
                }
            }
        }
    }

    private void checkFailedProgresses() {
        for (PublicationProgress progress : publicationProgressService.scanNonTerminalProgresses()) {
            if (ProgressStatus.PROCESSING.equals(progress.getStatus())) {
                String appIdStr = progress.getApplicationId();
                try {
                    ApplicationReport report = YarnUtils.getApplicationReport(yarnClient,
                            ApplicationId.fromString(appIdStr));
                    if (YarnApplicationState.FAILED.equals(report.getYarnApplicationState())) {
                        log.info("Found a running progress which is already failed.");
                        publicationProgressService.update(progress).fail("Yarn application failed.").commit();
                    }
                } catch (Exception e) {
                    log.error("Failed to get application report for appId" + appIdStr, e);
                    if (e.getMessage().contains("doesn't exist in the timeline store")) {
                        // RM was restarted while it is running
                        publicationProgressService.update(progress).fail(e.getMessage()).commit();
                    }
                }
            }
        }
    }

    private void killHangingJobs() {
        for (PublicationProgress progress : publicationProgressService.scanNonTerminalProgresses()) {
            if ((ProgressStatus.NEW.equals(progress.getStatus())
                    && progress.getLatestStatusUpdate().before(new Date(System.currentTimeMillis() - NEW_JOB_TIMEOUT)))
                    || (ProgressStatus.PROCESSING.equals(progress.getStatus()) && progress.getLatestStatusUpdate()
                            .before(new Date(System.currentTimeMillis() - RUNNING_JOB_TIMEOUT)))) {
                log.error("Found a hanging job " + progress + ". Kill it.");
                publicationProgressService.update(progress).fail("Time out at status " + progress.getStatus()).commit();
            }
        }
    }

    private List<PublicationProgress> scanForNewWorkFlow() {
        List<PublicationProgress> progresses = new ArrayList<>();
        Boolean serviceTenantBootstrapped = false;
        for (PublicationProgress progress : publicationProgressService.scanNonTerminalProgresses()) {
            try {
                if (!serviceTenantBootstrapped) {
                    dataCloudTenantService.bootstrapServiceTenant();
                    serviceTenantBootstrapped = true;
                }
                PublicationProgressUpdater updater = publicationProgressService.update(progress)
                        .status(ProgressStatus.PROCESSING);
                if (ProgressStatus.FAILED.equals(progress.getStatus())) {
                    updater.retry();
                }
                updater.commit();
                ApplicationId applicationId = submitWorkflow(progress);
                PublicationProgress progress1 = publicationProgressService.update(progress).applicationId(applicationId)
                        .commit();
                progresses.add(progress1);
                log.info("Send progress [" + progress + "] to workflow api: ApplicationID=" + applicationId);
            } catch (Exception e) {
                publicationProgressService.update(progress).status(ProgressStatus.FAILED).commit();
                // do not block scanning other progresses
                log.error("Failed to proceed progress " + progress, e);
            }
        }
        return progresses;
    }

    private ApplicationId submitWorkflow(PublicationProgress progress) {
        Publication publication = progress.getPublication();
        return new PublishWorkflowSubmitter() //
                .hdfsPodId(HdfsPodContext.getHdfsPodId()) //
                .workflowProxy(workflowProxy) //
                .progress(progress) //
                .publication(publication) //
                .submit();
    }

    @Override
    public ProgressStatus findProgressAtVersion(String publicationName, String version) {
        DataCloudEngineStage stage = new DataCloudEngineStage(DataCloudEngine.PUBLICATION, publicationName, version);
        return findProgressAtVersion(stage).getStatus();
    }

    @Override
    public DataCloudEngineStage findProgressAtVersion(DataCloudEngineStage stage) {
        stage.setEngine(DataCloudEngine.PUBLICATION);
        Publication publication = publicationEntityMgr.findByPublicationName(stage.getEngineName());
        if (publication == null) {
            throw new RuntimeException("Publication with name : " + stage.getEngineName() + " does not exist");
        }
        List<PublicationProgress> progressStatus = progressEntityMgr.findStatusByPublicationVersion(publication,
                stage.getVersion());
        if (CollectionUtils.isEmpty(progressStatus)) {
            stage.setStatus(ProgressStatus.NOTSTARTED);
            stage.setProgress(null);
            stage.setMessage(null);
        } else {
            PublicationProgress progress = progressStatus.get(0);
            stage.setStatus(progress.getStatus());
            stage.setProgress(progress.getProgress());
            stage.setMessage(progress.getErrorMessage());
        }
        return stage;
    }

    @Override
    public DataCloudEngine getEngine() {
        return DataCloudEngine.PUBLICATION;
    }

    @Override
    public String findCurrentVersion(String publicationName) {
        return null;
    }

}
