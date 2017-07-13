package com.latticeengines.datacloud.workflow.engine.steps;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.etl.publication.service.PublicationProgressService;
import com.latticeengines.datacloud.etl.publication.service.PublishConfigurationParser;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationConfiguration;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationDestination;
import com.latticeengines.domain.exposed.datacloud.publication.PublishTextToSqlConfiguration;
import com.latticeengines.domain.exposed.datacloud.publication.PublishToSqlConfiguration;
import com.latticeengines.domain.exposed.datacloud.publication.PublishToSqlConfiguration.PublicationStrategy;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.PublishConfiguration;
import com.latticeengines.proxy.exposed.sqoop.SqoopProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("publish")
@Scope("prototype")
public class Publish extends BaseWorkflowStep<PublishConfiguration> {

    public static final Logger log = LoggerFactory.getLogger(Publish.class);

    private static final Integer HANGING_THRESHOLD_HOURS = 24;

    private static final Integer MAX_ERRORS = 100;

    @Autowired
    private PublicationProgressService progressService;

    @Autowired
    private SqoopProxy sqoopProxy;

    @Autowired
    private PublishConfigurationParser configurationParser;

    private PublicationProgress progress;

    @Autowired
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

            if (pubConfig instanceof PublishTextToSqlConfiguration) {
                executePublishTextToSql((PublishTextToSqlConfiguration) pubConfig);
            } else if (pubConfig instanceof PublishToSqlConfiguration) {
                executePublishToSql((PublishToSqlConfiguration) pubConfig);
            }

        } catch (Exception e) {
            failByException(e);
        }
    }

    private void executePublishTextToSql(PublishTextToSqlConfiguration pubConfig) {
        log.info("Execute publish from text to sql.");
        pubConfig = (PublishTextToSqlConfiguration) configurationParser.parseSqlAlias(pubConfig);

        JdbcTemplate jdbcTemplate = configurationParser.getJdbcTemplate(pubConfig);
        log.info("Publication Strategy = " + pubConfig.getPublicationStrategy());

        if (pubConfig.getPublicationStrategy() != PublicationStrategy.APPEND) {
            throw new RuntimeException(
                    "Publish Text to SQL only support APPEND publication strategy");
        }

        SqoopExporter exporter = configurationParser.constructSqoopExporter(pubConfig, getConfiguration().getAvroDir());
        ApplicationId appId = ConverterUtils.toApplicationId(sqoopProxy.exportData(exporter).getApplicationIds().get(0));
        FinalApplicationStatus finalStatus = waitForApplicationToFinish(appId);
        if (FinalApplicationStatus.SUCCEEDED.equals(finalStatus)) {
            progress = progressService.update(progress).progress(0.95f).commit();
        } else {
            throw new RuntimeException("The final status is " + finalStatus + " instead of " + FinalApplicationStatus.SUCCEEDED);
        }

        Long count = configurationParser.countPublishedTable(pubConfig, jdbcTemplate);
        progress = progressService.update(progress).progress(1.0f).rowsPublished(count).status(ProgressStatus.FINISHED).commit();
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
        ApplicationId appId = ConverterUtils.toApplicationId(sqoopProxy.exportData(exporter).getApplicationIds().get(0));
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
                log.error(e.getMessage(), e);
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

}
