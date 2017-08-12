package com.latticeengines.datacloud.workflow.engine.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.etl.publication.service.PublicationProgressService;
import com.latticeengines.datacloud.etl.publication.service.PublishService;
import com.latticeengines.datacloud.etl.publication.service.impl.PublishServiceFactory;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationConfiguration;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationDestination;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.PublishConfiguration;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("publish")
@Scope("prototype")
public class Publish extends BaseWorkflowStep<PublishConfiguration> {

    public static final Logger log = LoggerFactory.getLogger(Publish.class);

    private final PublicationProgressService progressService;

    private PublicationProgress progress;

    @Autowired
    public Publish(PublicationProgressService progressService) {
        this.progressService = progressService;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute() {
        try {
            log.info("Inside Publish execute()");
            HdfsPodContext.changeHdfsPodId(getConfiguration().getHdfsPodId());
            progress = getConfiguration().getProgress();
            Publication publication = getConfiguration().getPublication();
            progress.setPublication(publication);

            progress = progressService.update(progress).progress(0.05f).status(ProgressStatus.PROCESSING).commit();

            PublicationConfiguration pubConfig = publication.getDestinationConfiguration();
            PublicationDestination destination = progress.getDestination();
            pubConfig.setDestination(destination);
            if (pubConfig.getDestination() == null) {
                throw new IllegalArgumentException("Publication destination is missing.");
            }
            PublishService publishService = PublishServiceFactory.getPublishServiceBean(publication.getPublicationType());
            progress = publishService.publish(progress, pubConfig);
        } catch (Exception e) {
            failByException(e);
        }
    }

    private void failByException(Exception e) {
        log.error("Failed to publish " + progress, e);
        progressService.update(progress).fail(e.getMessage()).commit();
    }

}
