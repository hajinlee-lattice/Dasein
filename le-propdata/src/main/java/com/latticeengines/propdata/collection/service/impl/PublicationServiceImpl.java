package com.latticeengines.propdata.collection.service.impl;

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
import com.latticeengines.domain.exposed.propdata.publication.PublicationConfiguration;
import com.latticeengines.domain.exposed.propdata.publication.PublicationDestination;
import com.latticeengines.domain.exposed.propdata.publication.PublicationRequest;
import com.latticeengines.propdata.collection.entitymgr.PublicationEntityMgr;
import com.latticeengines.propdata.collection.service.PublicationProgressService;
import com.latticeengines.propdata.collection.service.PublicationProgressUpdater;
import com.latticeengines.propdata.collection.service.PublicationService;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.service.SourceService;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("publicationService")
public class PublicationServiceImpl implements PublicationService {

    private static final Log log = LogFactory.getLog(PublicationServiceImpl.class);

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PublicationEntityMgr publicationEntityMgr;

    @Autowired
    private PublicationProgressService publicationProgressService;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Override
    public List<PublicationProgress> scan(String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            hdfsPathBuilder.changeHdfsPodId(hdfsPod);
        }
        return scanForNewWorkFlow(hdfsPod);
    }

    @Override
    public PublicationProgress publish(String publicationName, PublicationRequest request, String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            hdfsPathBuilder.changeHdfsPodId(hdfsPod);
        }
        Publication publication = publicationEntityMgr.findByPublicationName(publicationName);
        return publicationProgressService.publishVersion(publication, request.getSourceVersion(), request.getSubmitter());
    }

    private List<PublicationProgress> scanForNewWorkFlow(String hdfsPod) {
        List<PublicationProgress> progresses = new ArrayList<>();
        for (PublicationProgress progress : publicationProgressService.scanNonTerminalProgresses()) {
            try {
                ApplicationId applicationId = submitWorkflow(progress, hdfsPod);
                PublicationProgressUpdater updater = publicationProgressService.update(progress);
                if (PublicationProgress.Status.FAILED.equals(progress.getStatus())) {
                    updater.retry();
                }
                PublicationProgress progress1 = updater.applicationId(applicationId)
                        .status(PublicationProgress.Status.PUBLISHING).commit();
                progresses.add(progress1);
                log.info("Send progress [" + progress + "] to workflow api: ApplicationID=" + applicationId);
            } catch (Exception e) {
                // do not block scanning other progresses
                log.error(e);
            }
        }
        return progresses;
    }

    private ApplicationId submitWorkflow(PublicationProgress progress, String hdfsPod) {
        Publication publication = progress.getPublication();
        PublicationConfiguration publicationConfig = publication.getDestinationConfiguration();
        PublicationDestination destination = progress.getDestination();
        publicationConfig.setDestination(destination);
        return new PublicationWorkflowSubmitter() //
                .hdfsPodId(hdfsPod) //
                .workflowProxy(workflowProxy) //
                .publicationConfig(publicationConfig) //
                .submit();
    }

}
