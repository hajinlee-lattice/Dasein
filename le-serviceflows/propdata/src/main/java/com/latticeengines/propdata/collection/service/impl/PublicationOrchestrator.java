package com.latticeengines.propdata.collection.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.propdata.collection.service.PublicationExecutor;
import com.latticeengines.propdata.collection.service.PublicationProgressUpdater;
import com.latticeengines.propdata.collection.service.PublicationService;

@Component("publicationOrchestrator")
public class PublicationOrchestrator {

    private static final Log log = LogFactory.getLog(PublicationOrchestrator.class);

    @Autowired
    private PublicationService publicationService;

    @Autowired
    private PublicationExecutor executor;

    public void scan() {
        for (PublicationProgress progress : publicationService.scanNonTerminalProgresses()) {
            log.info("Sending progress [" + progress + "] to executor.");
            ApplicationId applicationId = executor.executeWorkflow(progress);
            PublicationProgressUpdater updater = publicationService.update(progress);
            if (PublicationProgress.Status.FAILED.equals(progress.getStatus())) {
                updater.retry();
            }
            updater.applicationId(applicationId).status(PublicationProgress.Status.PUBLISHING).commit();
        }
    }

}
