package com.latticeengines.propdata.core.datasource;

import org.springframework.jdbc.core.JdbcTemplate;

import com.latticeengines.domain.exposed.propdata.DataSourcePool;

public interface DataSourceService {

    JdbcTemplate getJdbcTemplateFromDbPool(DataSourcePool pool);

}
