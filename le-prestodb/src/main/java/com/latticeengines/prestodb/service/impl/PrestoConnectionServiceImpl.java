package com.latticeengines.prestodb.service.impl;

import java.beans.PropertyVetoException;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.prestodb.exposed.service.PrestoConnectionService;
import com.mchange.v2.c3p0.ComboPooledDataSource;

@Service
public class PrestoConnectionServiceImpl implements PrestoConnectionService {

    private static final Logger log = LoggerFactory.getLogger(PrestoConnectionServiceImpl.class);

    @Inject
    private EMRCacheService emrCacheService;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${aws.emr.cluster}")
    private String clusterName;

    @Override
    public String getClusterId() {
        if (Boolean.TRUE.equals(useEmr)) {
            return emrCacheService.getClusterId();
        } else {
            return "localhost";
        }
    }

    @Override
    public DataSource getPrestoDataSource() {
        String jdbcUrl = "jdbc:presto://" + getPrestoHostPort() + "/hive/default";
        log.info("Constructing c3p0 connection pool for " + jdbcUrl);
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        try {
            cpds.setDriverClass("com.facebook.presto.jdbc.PrestoDriver");
        } catch (PropertyVetoException e) {
            throw new RuntimeException(e);
        }
        cpds.setJdbcUrl(jdbcUrl);
        cpds.setUser(getPrestoUser());
        cpds.setPassword(null);

        cpds.setDataSourceName(getPrestoSourceName());
        cpds.setMinPoolSize(1);
        cpds.setInitialPoolSize(1);
        cpds.setMaxPoolSize(4);
        cpds.setAcquireIncrement(1);
        cpds.setCheckoutTimeout(60000);
        cpds.setMaxIdleTime(3600);
        cpds.setMaxIdleTimeExcessConnections(60);
        log.info("Created c3p0 connection pool for {}", cpds.toString());
        return cpds;
    }

    private String getPrestoHostPort() {
        if (Boolean.TRUE.equals(useEmr)) {
            String masterIp = emrCacheService.getMasterIp();
            return masterIp + ":8889";
        } else {
            return "localhost:8889";
        }
    }

    private String getPrestoUser() {
        return Boolean.TRUE.equals(useEmr) ? "hadoop" : "root";
    }

    private String getPrestoSourceName() {
        if (Boolean.TRUE.equals(useEmr)) {
            return "presto-" + clusterName;
        } else {
            return "presto-localhost";
        }
    }

}
