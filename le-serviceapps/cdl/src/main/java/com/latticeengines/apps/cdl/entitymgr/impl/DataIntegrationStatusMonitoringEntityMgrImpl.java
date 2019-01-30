package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.DataIntegrationStatusMonitoringDao;
import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.apps.cdl.repository.writer.DataIntegrationStatusMonitoringWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;

@Component("dataIntegrationStatusMonitorEntityMgr")
public class DataIntegrationStatusMonitoringEntityMgrImpl
        extends BaseEntityMgrRepositoryImpl<DataIntegrationStatusMonitor, Long>
        implements DataIntegrationStatusMonitoringEntityMgr {

    @Inject
    private DataIntegrationStatusMonitoringWriterRepository repository;

    @Inject
    private DataIntegrationStatusMonitoringDao dataIntegrationStatusMonitoringDao;

    @Inject
    private DataIntegrationStatusMonitoringEntityMgrImpl _self;

    @Override
    public BaseDao<DataIntegrationStatusMonitor> getDao() {
        return dataIntegrationStatusMonitoringDao;
    }

    @Override
    public BaseJpaRepository<DataIntegrationStatusMonitor, Long> getRepository() {
        return repository;
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public boolean createStatus(DataIntegrationStatusMonitor status) {
        dataIntegrationStatusMonitoringDao.create(status);
        return true;
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public DataIntegrationStatusMonitor getStatus(String eventId) {
        return dataIntegrationStatusMonitoringDao.findByField("WORKFLOW_REQ_ID", eventId);
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public DataIntegrationStatusMonitor updateStatus(DataIntegrationStatusMonitor status) {
        dataIntegrationStatusMonitoringDao.update(status);
        return status;
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public List<DataIntegrationStatusMonitor> getAllStatuses(Long tenantPid) {
        return dataIntegrationStatusMonitoringDao.findAllByField("FK_TENANT_ID", tenantPid);
    }

}
