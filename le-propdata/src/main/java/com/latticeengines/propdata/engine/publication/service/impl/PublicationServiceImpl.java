package com.latticeengines.propdata.engine.publication.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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

    @Override
    public List<PublicationProgress> scan(String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
        publishAll(hdfsPod);
        return scanForNewWorkFlow(hdfsPod);
    }

    @Override
    public PublicationProgress publish(String publicationName, PublicationRequest request, String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
        Publication publication = publicationEntityMgr.findByPublicationName(publicationName);
        PublicationProgress progress =  publicationProgressService.publishVersion(publication, request.getSourceVersion(),
                request.getSubmitter());
        if (progress == null) {
            log.info("There is already a progress for version " + request.getSourceVersion());
        }
        return progress;
    }

    private void publishAll(String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
        for (Publication publication : publicationEntityMgr.findAll()) {
            if (publication.isSchedularEnabled()) {
                try {
                    publicationProgressService.kickoffNewProgress(publication, PropDataConstants.SCAN_SUBMITTER);
                } catch (Exception e) {
                    log.error("Failed to trigger publication " + publication.getPublicationName());
                }
            }
        }
    }

    private List<PublicationProgress> scanForNewWorkFlow(String hdfsPod) {
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
                ApplicationId applicationId = submitWorkflow(progress, avroDir, hdfsPod);
                PublicationProgressUpdater updater = publicationProgressService.update(progress);
                if (PublicationProgress.Status.FAILED.equals(progress.getStatus())) {
                    updater.retry();
                }
                PublicationProgress progress1 = updater.applicationId(applicationId).commit();
                progresses.add(progress1);
                log.info("Send progress [" + progress + "] to workflow api: ApplicationID=" + applicationId);
            } catch (Exception e) {
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
