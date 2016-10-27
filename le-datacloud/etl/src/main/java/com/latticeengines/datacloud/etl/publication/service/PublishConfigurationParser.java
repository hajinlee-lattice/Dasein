package com.latticeengines.datacloud.etl.publication.service;

import org.springframework.jdbc.core.JdbcTemplate;

import com.latticeengines.domain.exposed.datacloud.publication.PublishTextToSqlConfiguration;
import com.latticeengines.domain.exposed.datacloud.publication.PublishToSqlConfiguration;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;

public interface PublishConfigurationParser {

    PublishToSqlConfiguration parseSqlAlias(PublishToSqlConfiguration sqlConfiguration);

    SqoopExporter constructSqoopExporter(PublishToSqlConfiguration sqlConfiguration, String avroDir);

    SqoopExporter constructSqoopExporter(PublishTextToSqlConfiguration textToSqlConfiguration, String textDir);

    String prePublishSql(PublishToSqlConfiguration sqlConfiguration, String sourceName);

    String postPublishSql(PublishToSqlConfiguration sqlConfiguration, String sourceName);

    Long countPublishedTable(PublishToSqlConfiguration sqlConfiguration, JdbcTemplate jdbcTemplate);

    JdbcTemplate getJdbcTemplate(PublishToSqlConfiguration sqlConfiguration);
}
