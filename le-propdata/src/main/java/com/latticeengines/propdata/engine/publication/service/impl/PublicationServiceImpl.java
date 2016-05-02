package com.latticeengines.propdata.engine.publication.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.domain.exposed.propdata.manage.Publication;
import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.domain.exposed.propdata.publication.PublicationRequest;
import com.latticeengines.propdata.core.PropDataConstants;
import com.latticeengines.propdata.core.service.PropDataTenantService;
import com.latticeengines.propdata.core.service.SourceService;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.publication.entitymgr.PublicationEntityMgr;
import com.latticeengines.propdata.engine.publication.service.PublicationProgressService;
import com.latticeengines.propdata.engine.publication.service.PublicationProgressUpdater;
import com.latticeengines.propdata.engine.publication.service.PublicationService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("publicationService")
public class PublicationServiceImpl implements PublicationService {

    private static final Log log = LogFactory.getLog(PublicationServiceImpl.class);

    private static final Long NEW_JOB_TIMEOUT = TimeUnit.MINUTES.toMillis(10);
    private static final Long RUNNING_JOB_TIMEOUT = TimeUnit.HOURS.toMillis(48);

    @Autowired
    private PublicationEntityMgr publicationEntityMgr;

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
    private Configuration yarnConfiguration;

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
                    ApplicationReport report = YarnUtils.getApplicationReport(yarnConfiguration,
                            ConverterUtils.toApplicationId(appIdStr));
                    if (YarnApplicationState.FAILED.equals(report.getYarnApplicationState())) {
                        log.info("Found a running progress which is already failed.");
                        publicationProgressService.update(progress).fail("Yarn application failed.").commit();
                    }
                } catch (Exception e) {
                    log.error("Failed to get application report for appId" + appIdStr, e);
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
                Source source = sourceService.findBySourceName(publication.getSourceName());
                String avroDir = hdfsPathBuilder.constructSnapshotDir(source, progress.getSourceVersion()).toString();
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

}
