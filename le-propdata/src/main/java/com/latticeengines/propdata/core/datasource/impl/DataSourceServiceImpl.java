package com.latticeengines.propdata.core.datasource.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.DataSourcePool;
import com.latticeengines.propdata.core.datasource.DataSourceConnection;
import com.latticeengines.propdata.core.datasource.DataSourceService;
import com.latticeengines.propdata.core.datasource.DataSourceUtils;
import com.latticeengines.propdata.core.service.ZkConfigurationService;

@Component
public class DataSourceServiceImpl implements DataSourceService {

    private final AtomicInteger roundRobinPos = new AtomicInteger(0);

    @Autowired
    private ZkConfigurationService zkConfigurationService;

    @Autowired
    @Qualifier("propdataScheduler")
    private ThreadPoolTaskScheduler scheduler;

    @PostConstruct
    private void postConstruct() {
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                cleanupJdbcTemplatePool();
            }
        }, TimeUnit.MINUTES.toMillis(10));
    }

    @Override
    public JdbcTemplate getJdbcTemplateFromDbPool(DataSourcePool pool) {
        return getJdbcTemplatesFromDbPool(pool, 1).get(0);
    }

    @Override
    public List<JdbcTemplate> getJdbcTemplatesFromDbPool(DataSourcePool pool, Integer num) {
        List<DataSourceConnection> connectionList = zkConfigurationService.getConnectionsInPool(pool);
        List<JdbcTemplate> jdbcTemplates = new ArrayList<>();
        synchronized (roundRobinPos) {
            for (int i = 0; i < num; i++) {
                DataSourceConnection connection = connectionList.get(roundRobinPos.get());
                jdbcTemplates.add(DataSourceUtils.getJdbcTemplate(connection));
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
