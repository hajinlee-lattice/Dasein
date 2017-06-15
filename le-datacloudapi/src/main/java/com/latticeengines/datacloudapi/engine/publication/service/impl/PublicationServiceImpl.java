package com.latticeengines.datacloudapi.engine.publication.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.service.PropDataTenantService;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.core.util.PropDataConstants;
import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationEntityMgr;
import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.datacloud.etl.publication.service.PublicationProgressService;
import com.latticeengines.datacloud.etl.publication.service.PublicationProgressUpdater;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloudapi.engine.publication.service.PublicationService;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngine;
import com.latticeengines.domain.exposed.datacloud.orchestration.EngineProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationRequest;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("publicationService")
public class PublicationServiceImpl implements PublicationService {

    private static final Log log = LogFactory.getLog(PublicationServiceImpl.class);

    private static final Long NEW_JOB_TIMEOUT = TimeUnit.MINUTES.toMillis(10);
    private static final Long RUNNING_JOB_TIMEOUT = TimeUnit.HOURS.toMillis(48);

    @Autowired
    private PublicationEntityMgr publicationEntityMgr;

    @Autowired
    private PublicationProgressEntityMgr progressEntityMgr;

    @Autowired
    private PublicationProgressService publicationProgressService;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private SourceService sourceService;

    @Autowired
    private PropDataTenantService propDataTenantService;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private YarnClient yarnClient;

    @Override
    public List<PublicationProgress> scan(String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
        checkFailedProgresses();
        killHangingJobs();
        publishAll();
        return scanForNewWorkFlow();
    }

    @Override
    public PublicationProgress publish(String publicationName, PublicationRequest request, String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
        Publication publication = publicationEntityMgr.findByPublicationName(publicationName);
        PublicationProgress progress = publicationProgressService.publishVersion(publication,
                request.getSourceVersion(), request.getSubmitter());
        if (progress == null) {
            log.info("There is already a progress for version " + request.getSourceVersion());
        }
        return progress;
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
                            ConverterUtils.toApplicationId(appIdStr));
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
                    propDataTenantService.bootstrapServiceTenant();
                    serviceTenantBootstrapped = true;
                }
                Publication publication = progress.getPublication();
                String avroDir = null;
                switch (publication.getMaterialType()) {
                    case SOURCE:
                        Source source = sourceService.findBySourceName(publication.getSourceName());
                        avroDir = hdfsPathBuilder.constructSnapshotDir(source, progress.getSourceVersion()).toString();
                        break;
                    case INGESTION:
                        avroDir = hdfsPathBuilder.constructIngestionDir(publication.getSourceName(),
                            progress.getSourceVersion()).toString();
                        break;
                }
                PublicationProgressUpdater updater = publicationProgressService.update(progress)
                        .status(ProgressStatus.PROCESSING);
                if (ProgressStatus.FAILED.equals(progress.getStatus())) {
                    updater.retry();
                }
                updater.commit();
                ApplicationId applicationId = submitWorkflow(progress, avroDir, HdfsPodContext.getHdfsPodId());
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

    private ApplicationId submitWorkflow(PublicationProgress progress, String avroDir, String hdfsPod) {
        Publication publication = progress.getPublication();
        return new PublishWorkflowSubmitter() //
                .hdfsPodId(hdfsPod) //
                .workflowProxy(workflowProxy) //
                .avroDir(avroDir) //
                .progress(progress) //
                .publication(publication) //
                .submit();
    }

    @Override
    public EngineProgress status(String publicationName, String version) {
        Publication publication = publicationEntityMgr.findByPublicationName(publicationName);
        if (publication == null) {
            throw new RuntimeException("Publication with name : " + publicationName + " does not exist");
        }
        List<PublicationProgress> progressStatus = progressEntityMgr.findStatusByPublicationVersion(publication,
                version);
        if (CollectionUtils.isEmpty(progressStatus)) {
            return new EngineProgress(DataCloudEngine.PUBLICATION, publication.getPublicationName(), version,
                    ProgressStatus.NOTSTARTED, 0.0f, null);
        } else {
            PublicationProgress progress = progressStatus.get(0);
            return new EngineProgress(DataCloudEngine.PUBLICATION, publication.getPublicationName(), version,
                    progress.getStatus(), progress.getProgress(), progress.getErrorMessage());
        }
    }

}
