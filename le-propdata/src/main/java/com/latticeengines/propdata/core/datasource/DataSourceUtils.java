package com.latticeengines.propdata.core.datasource;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.latticeengines.common.exposed.util.CipherUtils;

public class DataSourceUtils {

    public static DriverManagerDataSource getDataSource(DataSourceConnection dataSourceConnection) {
        DriverManagerDataSource driverManagerDataSource = new DriverManagerDataSource(
                dataSourceConnection.getUrl(), dataSourceConnection.getUsername(),
                CipherUtils.decrypt(dataSourceConnection.getPassword()));
        if (StringUtils.isNotEmpty(dataSourceConnection.getDriver())) {
            driverManagerDataSource.setDriverClassName(dataSourceConnection.getDriver());
        }
        return driverManagerDataSource;
    }

    public static JdbcTemplate getJdbcTemplate(DataSourceConnection connection) {
        DriverManagerDataSource dataSource = getDataSource(connection);
        return new JdbcTemplate(dataSource);
    }

}
