package com.latticeengines.propdata.core.datasource;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.latticeengines.common.exposed.util.CipherUtils;

public class DataSourceUtils {

    private static final Log log = LogFactory.getLog(DataSourceUtils.class);

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
        log.debug("Get jdbc for url " + dataSource.getUrl().substring(0, dataSource.getUrl().indexOf(";")));
        return jdbcTemplateMap.get(dataSource.getUrl());
    }

}
