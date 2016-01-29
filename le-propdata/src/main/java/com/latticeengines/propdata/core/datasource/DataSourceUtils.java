package com.latticeengines.propdata.core.datasource;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

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

    public static SQLDialect getSqlDialect(JdbcTemplate jdbcTemplate) {
        return getSqlDialect(jdbcTemplate.getDataSource());
    }

    public static SQLDialect getSqlDialect(DataSource dataSource) {
        try {
            Connection conn = dataSource.getConnection();
            SQLDialect dialect = SQLDialect.SQLSERVER;
            if (conn.getMetaData().getDatabaseProductName().toLowerCase().contains("mysql")) {
                dialect = SQLDialect.MYSQL;
            }
            conn.close();
            return dialect;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to parse DB product name from a DataSource", e);
        }
    }

}
