package com.latticeengines.propdata.core.datasource;

import org.springframework.jdbc.core.JdbcTemplate;

import com.latticeengines.domain.exposed.datacloud.DataSourcePool;

import java.util.List;

public interface DataSourceService {

    JdbcTemplate getJdbcTemplateFromDbPool(DataSourcePool pool);

    List<JdbcTemplate> getJdbcTemplatesFromDbPool(DataSourcePool pool, Integer num);

}
