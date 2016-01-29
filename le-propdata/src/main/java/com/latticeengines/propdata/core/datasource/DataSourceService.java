package com.latticeengines.propdata.core.datasource;

import org.springframework.jdbc.core.JdbcTemplate;

import com.latticeengines.propdata.core.datasource.Database;
import com.latticeengines.propdata.core.datasource.SQLDialect;

public interface DataSourceService {

    SQLDialect getSqlDialect(Database db);

    JdbcTemplate getJdbcTemplateFromDbPool(DataSourcePool pool);

}
