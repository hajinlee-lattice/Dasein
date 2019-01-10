package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.DataIntegrationStatusMonitoringDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;

@Component("dataIntegrationStatusMonitoringDao")
public class DataIntegrationStatusMonitoringDaoImpl extends
        BaseDaoImpl<DataIntegrationStatusMonitor> implements DataIntegrationStatusMonitoringDao {

    @Override
    protected Class<DataIntegrationStatusMonitor> getEntityClass() {
        return DataIntegrationStatusMonitor.class;
    }
}
