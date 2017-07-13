package com.latticeengines.datacloud.core.datasource.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.datasource.DataSourceConnection;
import com.latticeengines.datacloud.core.datasource.DataSourceService;
import com.latticeengines.datacloud.core.datasource.DataSourceUtils;
import com.latticeengines.datacloud.core.service.ZkConfigurationService;
import com.latticeengines.domain.exposed.datacloud.DataSourcePool;

@Component
public class DataSourceServiceImpl implements DataSourceService {

    private static final Logger log  = LoggerFactory.getLogger(DataSourceServiceImpl.class);
    private final AtomicInteger roundRobinPos = new AtomicInteger(0);

    @Autowired
    private ZkConfigurationService zkConfigurationService;

    @Autowired
    @Qualifier("commonTaskScheduler")
    private ThreadPoolTaskScheduler scheduler;

    @PostConstruct
    private void postConstruct() {
        scheduler.scheduleWithFixedDelay(() -> cleanupJdbcTemplatePool(), TimeUnit.MINUTES.toMillis(10));
    }

    @Override
    public JdbcTemplate getJdbcTemplateFromDbPool(DataSourcePool pool) {
        return getJdbcTemplatesFromDbPool(pool, 1).get(0);
    }

    @Override
    public List<JdbcTemplate> getJdbcTemplatesFromDbPool(DataSourcePool pool, Integer num) {
        List<DataSourceConnection> connectionList = zkConfigurationService.getConnectionsInPool(pool);
        List<JdbcTemplate> jdbcTemplates = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            try {
                DataSourceConnection connection = connectionList.get(roundRobinPos.get() % connectionList.size());
                jdbcTemplates.add(DataSourceUtils.getJdbcTemplate(connection));
            } catch (Exception e) {
                log.error("Failed to retrieve a jdbcTemplate from datasource pool", e);
            }
            synchronized (roundRobinPos) {
                Integer nextPos = (roundRobinPos.get() + 1) % connectionList.size();
                roundRobinPos.set(nextPos);
            }
        }
        return jdbcTemplates;
    }

    private void cleanupJdbcTemplatePool() {
        for (DataSourcePool pool: DataSourcePool.values()) {
            List<DataSourceConnection> connectionList = zkConfigurationService.getConnectionsInPool(pool);
            DataSourceUtils.retainUrls(connectionList);
        }
    }

}
