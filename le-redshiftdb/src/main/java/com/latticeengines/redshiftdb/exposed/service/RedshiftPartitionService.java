package com.latticeengines.redshiftdb.exposed.service;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;

public interface RedshiftPartitionService {

    String getLegacyPartition();
    String getDefaultPartition();

    DataSource getDataSource(String partition, String user);
    JdbcTemplate getJdbcTemplate(String partition, String user);
    RedshiftService getRedshiftService(String partition, String user);

    RedshiftService getBatchUserService(String partition);
    RedshiftService getSegmentUserService(String partition);
    JdbcTemplate getBatchUserJdbcTemplate(String partition);
    JdbcTemplate getSegmentUserJdbcTemplate(String partition);

}
