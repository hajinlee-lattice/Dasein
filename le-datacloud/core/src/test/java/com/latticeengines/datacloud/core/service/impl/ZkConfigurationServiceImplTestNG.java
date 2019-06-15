package com.latticeengines.datacloud.core.service.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.datasource.DataSourceConnection;
import com.latticeengines.datacloud.core.datasource.DataSourceUtils;
import com.latticeengines.datacloud.core.service.ZkConfigurationService;
import com.latticeengines.datacloud.core.testframework.DataCloudCoreFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataSourcePool;

public class ZkConfigurationServiceImplTestNG extends DataCloudCoreFunctionalTestNGBase {

    @Autowired
    private ZkConfigurationService zkConfigurationService;

    @Test(groups = "functional", enabled = false)
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
