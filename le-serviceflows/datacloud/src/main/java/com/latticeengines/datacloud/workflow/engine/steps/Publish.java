package com.latticeengines.datacloud.workflow.engine.steps;

import javax.inject.Inject;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.service.DataCloudNotificationService;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.datacloud.etl.publication.service.PublicationProgressService;
import com.latticeengines.datacloud.etl.publication.service.PublishService;
import com.latticeengines.datacloud.etl.publication.service.impl.PublishServiceFactory;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationConfiguration;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationDestination;
import com.latticeengines.domain.exposed.monitor.SlackSettings;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.PublishConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("publish")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class Publish extends BaseWorkflowStep<PublishConfiguration> {

    public static final Logger log = LoggerFactory.getLogger(Publish.class);

    private static final String SLACK_BOT = "SourcePublisher";

    @Inject
    private DataCloudNotificationService notificationService;

    @Inject
    private PublicationProgressEntityMgr publicationProgressEntityMgr;

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
            long startTime = System.currentTimeMillis();
            HdfsPodContext.changeHdfsPodId(getConfiguration().getHdfsPodId());
            progress = getConfiguration().getProgress();
            // To populate ApplicationId from database
            progress = publicationProgressEntityMgr.findByPid(progress.getPid());
            Publication publication = getConfiguration().getPublication();
            progress.setPublication(publication);

            notificationService.sendSlack(
                    String.format("%s [%s]", publication.getPublicationName(), progress.getApplicationId()),
                    String.format("Version %s starts to publish", progress.getSourceVersion()), SLACK_BOT,
                    SlackSettings.Color.NORMAL);

            progress = progressService.update(progress).progress(0.05f).status(ProgressStatus.PROCESSING).commit();

            PublicationConfiguration pubConfig = publication.getDestinationConfiguration();
            PublicationDestination destination = progress.getDestination();
            pubConfig.setDestination(destination);
            if (pubConfig.getDestination() == null) {
                throw new IllegalArgumentException("Publication destination is missing.");
            }
            @SuppressWarnings("rawtypes")
            PublishService publishService = PublishServiceFactory
                    .getPublishServiceBean(publication.getPublicationType());
            progress = publishService.publish(progress, pubConfig);

            notificationService.sendSlack(
                    String.format("%s [%s]", publication.getPublicationName(), progress.getApplicationId()),
                    String.format("Version %s is published after %s :clap:", progress.getSourceVersion(),
                            DurationFormatUtils.formatDurationHMS(System.currentTimeMillis() - startTime)),
                    SLACK_BOT, SlackSettings.Color.GOOD);
        } catch (Exception e) {
            failByException(e);
        }
    }

    private void failByException(Exception e) {
        log.error("Failed to publish " + progress, e);
        notificationService.sendSlack(
                String.format("%s [%s]", progress.getPublication().getPublicationName(), progress.getApplicationId()),
                String.format("Version %s is failed :sob:", progress.getSourceVersion()), SLACK_BOT,
                SlackSettings.Color.DANGER);
        progressService.update(progress).fail(e.getMessage()).commit();
    }

}
