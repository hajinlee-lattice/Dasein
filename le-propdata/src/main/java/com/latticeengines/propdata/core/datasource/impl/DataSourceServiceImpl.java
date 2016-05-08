package com.latticeengines.propdata.core.datasource.impl;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.DataSourcePool;
import com.latticeengines.propdata.core.datasource.DataSourceConnection;
import com.latticeengines.propdata.core.datasource.DataSourceService;
import com.latticeengines.propdata.core.datasource.DataSourceUtils;
import com.latticeengines.propdata.core.service.ZkConfigurationService;

@Component
public class DataSourceServiceImpl implements DataSourceService {

    private Log log = LogFactory.getLog(this.getClass());

    private final AtomicInteger roundRobinPos = new AtomicInteger(0);

    @Autowired
    private ZkConfigurationService zkConfigurationService;

    @Override
    public JdbcTemplate getJdbcTemplateFromDbPool(DataSourcePool pool) {
        List<DataSourceConnection> connectionList = zkConfigurationService.getConnectionsInPool(pool);
        synchronized (roundRobinPos) {
            Integer nextPos = (roundRobinPos.get() + 1) % connectionList.size();
            roundRobinPos.set(nextPos);
        }
        DataSourceConnection connection = connectionList.get(roundRobinPos.get());
        return DataSourceUtils.getJdbcTemplate(connection);
    }

}
