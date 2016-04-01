package com.latticeengines.propdata.core.service.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testng.annotations.Test;

import com.latticeengines.propdata.core.datasource.DataSourceConnection;
import com.latticeengines.domain.exposed.propdata.DataSourcePool;
import com.latticeengines.propdata.core.datasource.DataSourceUtils;
import com.latticeengines.propdata.core.service.ZkConfigurationService;
import com.latticeengines.propdata.core.testframework.PropDataCoreFunctionalTestNGBase;


public class ZkConfigurationServiceImplTestNG extends PropDataCoreFunctionalTestNGBase {

    @Autowired
    private ZkConfigurationService zkConfigurationService;

    @Test(groups = "functional")
    public void testConnectionPool() throws SQLException {
        for (DataSourcePool pool: DataSourcePool.values()) {
            List<DataSourceConnection> connectionList =
                    zkConfigurationService.getConnectionsInPool(pool);

            for (DataSourceConnection connection: connectionList) {
                DriverManagerDataSource dataSource = DataSourceUtils.getDataSource(connection);
                Connection conn = dataSource.getConnection();
                conn.close();
            }
        }

    }

}
