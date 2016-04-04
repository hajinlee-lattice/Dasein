package com.latticeengines.propdata.core.datasource;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.latticeengines.common.exposed.util.CipherUtils;

public class DataSourceUtils {

    private static ConcurrentMap<String, JdbcTemplate> jdbcTemplateMap = new ConcurrentHashMap<>();

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
        if (!jdbcTemplateMap.containsKey(dataSource.getUrl())) {
            jdbcTemplateMap.putIfAbsent(dataSource.getUrl(), new JdbcTemplate(dataSource));
        }
        return jdbcTemplateMap.get(dataSource.getUrl());
    }

}
