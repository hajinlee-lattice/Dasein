package com.latticeengines.propdata.engine.publication.service;

import org.springframework.jdbc.core.JdbcTemplate;

import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.propdata.publication.PublishToSqlConfiguration;

public interface PublishConfigurationParser {

    PublishToSqlConfiguration parseSqlAlias(PublishToSqlConfiguration sqlConfiguration);

    SqoopExporter constructSqoopExporter(PublishToSqlConfiguration sqlConfiguration, String avroDir);

    String prePublishSql(PublishToSqlConfiguration sqlConfiguration, String sourceName);

    String postPublishSql(PublishToSqlConfiguration sqlConfiguration, String sourceName);

    Long countPublishedTable(PublishToSqlConfiguration sqlConfiguration, JdbcTemplate jdbcTemplate);

    JdbcTemplate getJdbcTemplate(PublishToSqlConfiguration sqlConfiguration);
}
