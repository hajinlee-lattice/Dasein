package com.latticeengines.datacloud.etl.publication.service.impl;

import javax.annotation.PostConstruct;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.latticeengines.datacloud.etl.publication.service.PublishConfigurationParser;
import com.latticeengines.datacloud.etl.publication.service.PublishService;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublishTextToSqlConfiguration;
import com.latticeengines.domain.exposed.datacloud.publication.PublishToSqlConfiguration;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.proxy.exposed.sqoop.SqoopProxy;

@Service("sqlPublishService")
public class SqlPublishService extends AbstractPublishService implements PublishService<PublishToSqlConfiguration> {

    private static Logger log = LoggerFactory.getLogger(SqlPublishService.class);

    @Autowired
    private PublishConfigurationParser configurationParser;

    @Autowired
    private SqoopProxy sqoopProxy;

    @PostConstruct
    private void postConstruct() {
        PublishServiceFactory.register(PublishToSqlConfiguration.class, this);
        PublishServiceFactory.register(PublishTextToSqlConfiguration.class, this);
    }

    @Override
    public PublicationProgress publish(PublicationProgress progress, PublishToSqlConfiguration configuration) {
        if (configuration instanceof PublishTextToSqlConfiguration) {
            return publishText(progress, (PublishTextToSqlConfiguration) configuration);
        } else {
            return publishAvro(progress, configuration);
        }
    }

    private PublicationProgress publishAvro(PublicationProgress progress, PublishToSqlConfiguration configuration) {
        log.info("Execute publish to sql.");
        configuration = configurationParser.parseSqlAlias(configuration);

        JdbcTemplate jdbcTemplate = configurationParser.getJdbcTemplate(configuration);
        log.info("Publication Strategy = " + configuration.getPublicationStrategy());
        switch (configuration.getPublicationStrategy()) {
        case VERSIONED:
        case REPLACE:
            String preSql = configurationParser.prePublishSql(configuration, configuration.getSourceName());
            log.info("Executing pre publish sql: " + preSql);
            jdbcTemplate.execute(preSql);
            break;
        case APPEND:
            break;
        }

        SqoopExporter exporter = configurationParser.constructSqoopExporter(configuration, configuration.getAvroDir());
        ApplicationId appId = ConverterUtils
                .toApplicationId(sqoopProxy.exportData(exporter).getApplicationIds().get(0));
        FinalApplicationStatus finalStatus = waitForApplicationToFinish(appId, progress);
        if (FinalApplicationStatus.SUCCEEDED.equals(finalStatus)) {
            progress = progressService.update(progress).progress(0.95f).commit();
        } else {
            throw new RuntimeException(
                    "The final status is " + finalStatus + " instead of " + FinalApplicationStatus.SUCCEEDED);
        }

        String postSql = configurationParser.postPublishSql(configuration, configuration.getSourceName());
        log.info("Executing post publish sql: " + postSql);
        jdbcTemplate.execute(postSql);
        Long count = configurationParser.countPublishedTable(configuration, jdbcTemplate);
        progress = progressService.update(progress).progress(1.0f).rowsPublished(count).status(ProgressStatus.FINISHED)
                .commit();
        return progress;
    }

    private PublicationProgress publishText(PublicationProgress progress, PublishTextToSqlConfiguration configuration) {
        log.info("Execute publish from text to sql.");
        configuration = configurationParser.parseSqlAlias(configuration);

        JdbcTemplate jdbcTemplate = configurationParser.getJdbcTemplate(configuration);
        log.info("Publication Strategy = " + configuration.getPublicationStrategy());

        if (configuration.getPublicationStrategy() != PublishToSqlConfiguration.PublicationStrategy.APPEND) {
            throw new RuntimeException(
                    "Publish Text to SQL only support APPEND publication strategy");
        }

        SqoopExporter exporter = configurationParser.constructSqoopExporter(configuration, configuration.getAvroDir());
        ApplicationId appId = ConverterUtils.toApplicationId(sqoopProxy.exportData(exporter).getApplicationIds().get(0));
        FinalApplicationStatus finalStatus = waitForApplicationToFinish(appId, progress);
        if (FinalApplicationStatus.SUCCEEDED.equals(finalStatus)) {
            progress = progressService.update(progress).progress(0.95f).commit();
        } else {
            throw new RuntimeException("The final status is " + finalStatus + " instead of " + FinalApplicationStatus.SUCCEEDED);
        }

        Long count = configurationParser.countPublishedTable(configuration, jdbcTemplate);
        progress = progressService.update(progress).progress(1.0f).rowsPublished(count).status(ProgressStatus.FINISHED).commit();
        return progress;
    }

}
