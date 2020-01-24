package com.latticeengines.datacloud.etl.publication.service.impl;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.publication.service.PublishService;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationConfiguration;
import com.latticeengines.domain.exposed.datacloud.publication.PublishTextToSqlConfiguration;
import com.latticeengines.domain.exposed.datacloud.publication.PublishToSqlConfiguration;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.proxy.exposed.sqoop.SqoopProxy;

@Service("sqlPublishService")
public class SqlPublishService extends AbstractPublishService implements PublishService<PublishToSqlConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SqlPublishService.class);

    @Inject
    private SqoopProxy sqoopProxy;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @PostConstruct
    private void postConstruct() {
        PublishServiceFactory.register(Publication.PublicationType.SQL, this);
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

        String sourceName = progress.getPublication().getSourceName();
        log.info("SourceName = " + sourceName);

        JdbcTemplate jdbcTemplate = configurationParser.getJdbcTemplate(configuration);
        log.info("Publication Strategy = " + configuration.getPublicationStrategy());
        switch (configuration.getPublicationStrategy()) {
        case VERSIONED:
        case REPLACE:
            String preSql = configurationParser.prePublishSql(configuration, sourceName);
            log.info("Executing pre publish sql: " + preSql);
            jdbcTemplate.execute(preSql);
            break;
        case APPEND:
            break;
        }

        String avroDir = getAvroDir(progress);
        if (StringUtils.isBlank(avroDir)) {
            throw new RuntimeException("Cannot find avro dir for publication progress " + progress);
        }
        SqoopExporter exporter = configurationParser.constructSqoopExporter(configuration, avroDir);
        ApplicationId appId = ConverterUtils
                .toApplicationId(sqoopProxy.exportData(exporter).getApplicationIds().get(0));
        FinalApplicationStatus finalStatus = waitForApplicationToFinish(appId, progress);
        if (FinalApplicationStatus.SUCCEEDED.equals(finalStatus)) {
            progress = progressService.update(progress).progress(0.95f).commit();
        } else {
            throw new RuntimeException(
                    "The final status is " + finalStatus + " instead of " + FinalApplicationStatus.SUCCEEDED);
        }

        String postSql = configurationParser.postPublishSql(configuration, sourceName);
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

        if (configuration.getPublicationStrategy() != PublicationConfiguration.PublicationStrategy.APPEND) {
            throw new RuntimeException(
                    "Publish Text to SQL only support APPEND publication strategy");
        }
        String avroDir = getAvroDir(progress);
        if (StringUtils.isBlank(avroDir)) {
            throw new RuntimeException("Cannot find avro dir for publication progress " + progress);
        }
        SqoopExporter exporter = configurationParser.constructSqoopExporter(configuration, avroDir);
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

    private String getAvroDir(PublicationProgress progress) {
        Publication publication = progress.getPublication();
        String sourceName = publication.getSourceName();
        String avroDir = null;
        switch (publication.getMaterialType()) {
            case SOURCE:
                avroDir = hdfsPathBuilder.constructSnapshotDir(sourceName, progress.getSourceVersion()).toString();
                break;
            case INGESTION:
                avroDir = hdfsPathBuilder
                        .constructIngestionDir(publication.getSourceName(), progress.getSourceVersion()).toString();
                break;
        }
        return avroDir;
    }

}
