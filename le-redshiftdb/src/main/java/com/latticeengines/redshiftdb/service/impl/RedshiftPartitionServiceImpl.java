package com.latticeengines.redshiftdb.service.impl;

import java.beans.PropertyVetoException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.bean.BeanFactoryEnvironment;
import com.latticeengines.redshiftdb.exposed.service.RedshiftPartitionService;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.mchange.v2.c3p0.ComboPooledDataSource;

@Service
public class RedshiftPartitionServiceImpl implements RedshiftPartitionService {

    private static final Logger log = LoggerFactory.getLogger(RedshiftPartitionServiceImpl.class);

    private static final String PARTITION_TOKEN = "{{PARTITION}}";

    private static final ConcurrentMap<String, ComboPooledDataSource> dataSources = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, RedshiftService> services = new ConcurrentHashMap<>();

    @Value("${aws.default.access.key}")
    private String awsAccessKey;

    @Value("${aws.default.secret.key.encrypted}")
    private String awsSecretKey;

    @Value("${redshift.root.user}")
    private String batchUser;

    @Value("${redshift.segment.user}")
    private String segmentUser;

    @Value("${redshift.password.encrypted}")
    private String password;

    @Value("${redshift.legacy.partition}")
    private String legacyPartition;

    @Value("${redshift.default.partition}")
    private String defaultPartition;

    @Value("${redshift.jdbc.url.pattern}")
    private String jdbcUrlPattern;

    @Value("${common.le.environment}")
    private String leEnv;

    @Override
    public String getLegacyPartition() {
        return legacyPartition;
    }

    @Override
    public String getDefaultPartition() {
        return defaultPartition;
    }

    @Override
    public DataSource getDataSource(String partition, String user) {
        String cacheKey = getCacheKey(partition, user);
        if (!dataSources.containsKey(cacheKey)) {
            dataSources.put(cacheKey, createDataSource(partition, user, password));
        }
        return dataSources.get(cacheKey);
    }

    @Override
    public JdbcTemplate getJdbcTemplate(String partition, String user) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate();
        jdbcTemplate.setDataSource(getDataSource(partition, user));
        return jdbcTemplate;
    }

    @Override
    public RedshiftService getRedshiftService(String partition, String user) {
        String cacheKey = getCacheKey(partition, user);
        if (!services.containsKey(cacheKey)) {
            services.put(cacheKey, createService(partition, user));
        }
        return services.get(cacheKey);
    }

    @Override
    public RedshiftService getBatchUserService(String partition) {
        return getRedshiftService(partition, batchUser);
    }


    @Override
    public RedshiftService getSegmentUserService(String partition) {
        return getRedshiftService(partition, segmentUser);
    }

    @Override
    public JdbcTemplate getBatchUserJdbcTemplate(String partition) {
        return getJdbcTemplate(partition, batchUser);
    }

    @Override
    public JdbcTemplate getSegmentUserJdbcTemplate(String partition) {
        return getJdbcTemplate(partition, segmentUser);
    }

    private synchronized RedshiftService createService(String partition, String user) {
        if (StringUtils.isBlank(partition)) {
            partition = defaultPartition;
        }
        String cacheKey = getCacheKey(partition, user);
        if (services.containsKey(cacheKey)) {
            return services.get(cacheKey);
        }
        return new RedshiftServiceImpl(awsAccessKey, awsSecretKey, partition, getJdbcTemplate(partition, user));
    }

    private synchronized ComboPooledDataSource createDataSource(String partition, String user, String password) {
        String cacheKey = getCacheKey(partition, user);
        if (dataSources.containsKey(cacheKey)) {
            return dataSources.get(cacheKey);
        }

        String jdbcUrl = getJdbcUrl(partition);
        log.info("Constructing c3p0 connection pool for " + jdbcUrl);
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        try {
            cpds.setDriverClass("com.amazon.redshift.jdbc42.Driver");
        } catch (PropertyVetoException e) {
            throw new RuntimeException(e);
        }
        cpds.setJdbcUrl(jdbcUrl);
        cpds.setUser(user);
        cpds.setPassword(password);

        // Give a meaningful name for better troubleshooting
        String dbName;
        try {
            dbName = jdbcUrl.substring(jdbcUrl.lastIndexOf("/"),
                    jdbcUrl.indexOf("?", jdbcUrl.lastIndexOf("/")));
        } catch (Exception e) {
            dbName = jdbcUrl.substring(0, jdbcUrl.lastIndexOf("/"));
        }

        BeanFactoryEnvironment.Environment currentEnv = BeanFactoryEnvironment.getEnvironment();
        cpds.setDataSourceName(
                String.format("%s-%s", currentEnv, dbName.replaceAll("[^A-Za-z0-9]", "")));
        cpds.setMinPoolSize(0);
        cpds.setInitialPoolSize(0);
        cpds.setMaxPoolSize(8);
        cpds.setAcquireIncrement(1);

        cpds.setCheckoutTimeout(300000);
        cpds.setMaxIdleTime(10);

        log.info("Created c3p0 connection pool for {}", cpds.toString());
        return cpds;
    }

    private String getCacheKey(String partition, String user) {
        if (StringUtils.isBlank(partition)) {
            partition = defaultPartition;
        }
        if (legacyPartition.equals(partition) && !"prodcluster".equals(leEnv)) {
            throw new IllegalArgumentException("Should not be querying the legacy partition.");
        }
        return user + "@" + partition;
    }

    private String getJdbcUrl(String partition) {
        partition = StringUtils.isBlank(partition) ? defaultPartition : partition;
        return jdbcUrlPattern.replace(PARTITION_TOKEN, partition);
    }
}
