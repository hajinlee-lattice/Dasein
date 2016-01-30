package com.latticeengines.propdata.core.datasource.impl;

import java.util.List;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.datasource.DataSourceConnection;
import com.latticeengines.propdata.core.datasource.DataSourcePool;
import com.latticeengines.propdata.core.datasource.DataSourceService;
import com.latticeengines.propdata.core.datasource.DataSourceUtils;
import com.latticeengines.propdata.core.datasource.Database;
import com.latticeengines.propdata.core.datasource.SQLDialect;
import com.latticeengines.propdata.core.service.ZkConfigurationService;

@Component
public class DataSourceServiceImpl implements DataSourceService {

    @Autowired
    @Qualifier(value = "propDataManage")
    private DataSource manageDataSource;

    private static int roundRobinPos = 0;

    @Autowired
    private ZkConfigurationService zkConfigurationService;

    @Override
    public JdbcTemplate getJdbcTemplateFromDbPool(DataSourcePool pool) {
        List<DataSourceConnection> connectionList = zkConfigurationService.getConnectionsInPool(pool);
        DataSourceConnection connection = connectionList.get(roundRobinPos);
        roundRobinPos = (roundRobinPos + 1) % connectionList.size();
        return DataSourceUtils.getJdbcTemplate(connection);
    }

    @Override
    public SQLDialect getSqlDialect(Database db) {
        switch (db) {
            case ManageDB:
                return DataSourceUtils.getSqlDialect(manageDataSource);
            default:
                return SQLDialect.SQLSERVER;
        }
    }

}
