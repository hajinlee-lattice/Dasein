package com.latticeengines.propdata.workflow.engine.steps;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.domain.exposed.propdata.manage.Publication;
import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.domain.exposed.propdata.publication.PublicationConfiguration;
import com.latticeengines.domain.exposed.propdata.publication.PublicationDestination;
import com.latticeengines.domain.exposed.propdata.publication.PublishToSqlConfiguration;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.engine.publication.service.PublicationProgressService;
import com.latticeengines.propdata.engine.publication.service.PublishConfigurationParser;
import com.latticeengines.proxy.exposed.propdata.InternalProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("publish")
@Scope("prototype")
public class Publish extends BaseWorkflowStep<PublishConfiguration> {

    public static final Log log = LogFactory.getLog(Publish.class);

    private static final Integer HANGING_THRESHOLD_HOURS = 24;

    private static final Integer MAX_ERRORS = 100;

    @Autowired
    private PublicationProgressService progressService;

    @Autowired
    private InternalProxy internalProxy;

    @Autowired
    private PublishConfigurationParser configurationParser;

    private PublicationProgress progress;

    private YarnClient yarnClient;

    private String sourceName;

    @Override
    public void execute() {
        try {
            log.info("Inside Publish execute()");
            HdfsPodContext.changeHdfsPodId(getConfiguration().getHdfsPodId());
            progress = getConfiguration().getProgress();
            Publication publication = getConfiguration().getPublication();
            progress.setPublication(publication);

            sourceName = getConfiguration().getPublication().getSourceName();
            progress = progressService.update(progress).progress(0.05f).status(ProgressStatus.PROCESSING).commit();

            PublicationConfiguration pubConfig = publication.getDestinationConfiguration();
            PublicationDestination destination = progress.getDestination();
            pubConfig.setDestination(destination);
            if (pubConfig.getDestination() == null) {
                throw new IllegalArgumentException("Publication destination is missing.");
            }

            initializeYarnClient();

            if (pubConfig instanceof PublishToSqlConfiguration) {
                executePublishToSql((PublishToSqlConfiguration) pubConfig);
            }

        } catch (Exception e) {
            failByException(e);
        } finally {
            try {
                yarnClient.close();
            } catch (Exception e) {
                log.error(e);
            }
        }
    }

    private void executePublishToSql(PublishToSqlConfiguration sqlConfiguration) {
        log.info("Execute publish to sql.");
        sqlConfiguration = configurationParser.parseSqlAlias(sqlConfiguration);

        JdbcTemplate jdbcTemplate = configurationParser.getJdbcTemplate(sqlConfiguration);
        log.info("Publication Strategy = " + sqlConfiguration.getPublicationStrategy());
        switch (sqlConfiguration.getPublicationStrategy()) {
            case VERSIONED:
            case REPLACE:
                String preSql = configurationParser.prePublishSql(sqlConfiguration, sourceName);
                log.info("Executing pre publish sql: " + preSql);
                jdbcTemplate.execute(preSql);
                break;
            case APPEND:
                break;
        }

        SqoopExporter exporter = configurationParser.constructSqoopExporter(sqlConfiguration,
                getConfiguration().getAvroDir());
        AppSubmission appSub = internalProxy.exportTable(exporter);
        ApplicationId appId = ConverterUtils.toApplicationId(appSub.getApplicationIds().get(0));
        FinalApplicationStatus finalStatus = waitForApplicationToFinish(appId);
        if (FinalApplicationStatus.SUCCEEDED.equals(finalStatus)) {
            progress = progressService.update(progress).progress(0.95f).commit();
        } else {
            throw new RuntimeException(
                    "The final status is " + finalStatus + " instead of " + FinalApplicationStatus.SUCCEEDED);
        }

        String postSql = configurationParser.postPublishSql(sqlConfiguration, sourceName);
        log.info("Executing post publish sql: " + postSql);
        jdbcTemplate.execute(postSql);
        Long count = configurationParser.countPublishedTable(sqlConfiguration, jdbcTemplate);
        progress = progressService.update(progress).progress(1.0f).rowsPublished(count).status(ProgressStatus.FINISHED)
                .commit();
    }

    private FinalApplicationStatus waitForApplicationToFinish(ApplicationId appId) {
        Long timeout = TimeUnit.HOURS.toMillis(HANGING_THRESHOLD_HOURS);
        int errors = 0;
        FinalApplicationStatus status = FinalApplicationStatus.UNDEFINED;
        do {
            try {
                ApplicationReport report = yarnClient.getApplicationReport(appId);
                status = report.getFinalApplicationStatus();
                YarnApplicationState state = report.getYarnApplicationState();
                Float appProgress = report.getProgress();
                String logMessage = String.format("Application [%s] is at state [%s]", appId, state);
                if (YarnApplicationState.RUNNING.equals(state)) {
                    logMessage += String.format(": %.2f ", appProgress * 100) + "%";
                }
                log.info(logMessage);

                appProgress = convertToOverallProgress(report.getProgress());
                Float dbProgress = progress.getProgress();
                if (!appProgress.equals(dbProgress)) {
                    progress = progressService.update(progress).progress(appProgress).commit();
                } else {
                    Long lastUpdate = progress.getLatestStatusUpdate().getTime();
                    if (System.currentTimeMillis() - lastUpdate >= timeout) {
                        String errorMsg = "The process has been hanging for " + DurationFormatUtils
                                .formatDurationWords(System.currentTimeMillis() - lastUpdate, true, false) + ".";
                        throw new RuntimeException(errorMsg);
                    }
                }
            } catch (Exception e) {
                log.error(e);
                errors++;
                if (errors >= MAX_ERRORS) {
                    throw new RuntimeException("Exceeded maximum error allowance.", e);
                }
            } finally {
                try {
                    Thread.sleep(10000L);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        } while (!YarnUtils.TERMINAL_STATUS.contains(status));
        return status;
    }

    private Float convertToOverallProgress(Float applicationProgress) {
        return applicationProgress * 0.9f + 0.05f;
    }

    private void failByException(Exception e) {
        log.error("Failed to publish " + progress, e);
        progressService.update(progress).fail(e.getMessage()).commit();
    }

    private void initializeYarnClient() {
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();
    }

}
