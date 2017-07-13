package com.latticeengines.datacloud.core.datasource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.latticeengines.common.exposed.util.CipherUtils;

public class DataSourceUtils {

    private static final Logger log = LoggerFactory.getLogger(DataSourceUtils.class);

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

    public static void retainUrls(Collection<DataSourceConnection> conns) {
        List<String> urlsToRetain = new ArrayList<>();
        if (conns != null) {
            for (DataSourceConnection conn : conns) {
                urlsToRetain.add(conn.getUrl());
            }
            Set<String> urlsToRemove = new HashSet<>(jdbcTemplateMap.keySet());
            urlsToRemove.removeAll(urlsToRetain);
            for (String url : urlsToRemove) {
                jdbcTemplateMap.remove(url);
            }
        }
    }

}
