package com.latticeengines.propdata.collection.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.Publication;
import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.propdata.collection.entitymgr.PublicationEntityMgr;
import com.latticeengines.propdata.collection.service.PublicationProgressService;
import com.latticeengines.propdata.collection.service.PublicationProgressUpdater;
import com.latticeengines.propdata.collection.service.PublicationService;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.source.Source;

@Component("publicationService")
public class PublicationServiceImpl implements PublicationService {

    private static final Log log = LogFactory.getLog(PublicationServiceImpl.class);

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PublicationEntityMgr publicationEntityMgr;

    @Autowired
    private PublicationProgressService publicationProgressService;

    @Override
    public void scan() {
        for (PublicationProgress progress : publicationProgressService.scanNonTerminalProgresses()) {
            ApplicationId applicationId = submitWorkflow(progress);
            PublicationProgressUpdater updater = publicationProgressService.update(progress);
            if (PublicationProgress.Status.FAILED.equals(progress.getStatus())) {
                updater.retry();
            }
            updater.applicationId(applicationId).status(PublicationProgress.Status.PUBLISHING).commit();
            log.info("Send progress [" + progress + "] to workflow api: ApplicationID=" + applicationId);
        }
    }

    @Override
    public void publish(Source source, String creator) {
        String currentVersion = hdfsSourceEntityMgr.getCurrentVersion(source);
        for (Publication publication : publicationEntityMgr.findBySourceName(source.getSourceName())) {
            publicationProgressService.publishVersion(publication, currentVersion, creator);
        }
    }

    private ApplicationId submitWorkflow(PublicationProgress progress) {
        return null;
    }

}
