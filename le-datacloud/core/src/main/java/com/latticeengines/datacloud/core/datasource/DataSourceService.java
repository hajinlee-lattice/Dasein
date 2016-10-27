package com.latticeengines.datacloud.core.datasource;

import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;

import com.latticeengines.domain.exposed.datacloud.DataSourcePool;

public interface DataSourceService {

    JdbcTemplate getJdbcTemplateFromDbPool(DataSourcePool pool);

    List<JdbcTemplate> getJdbcTemplatesFromDbPool(DataSourcePool pool, Integer num);

}
