package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.DataIntegrationStatusMonitoringDao;
import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.apps.cdl.repository.DataIntegrationStatusMonitoringRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;

/**
 * This Class Doesnot use MultiTenantContext. Because, when we receive the StatusMessages from Tray, what we get is workflowRequestId.
 * We will get bunch of such messages in one call. It does look into DataIntegrationStatusMonitoring for different workflowReqIds.
 * These workflowRequestIds span across multiple tenants. So we cannot use MultiTenantContext in this scenario.
 */
@Component("dataIntegrationStatusMonitorEntityMgr")
public class DataIntegrationStatusMonitoringEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<DataIntegrationStatusMonitoringRepository, DataIntegrationStatusMonitor, Long>
        implements DataIntegrationStatusMonitoringEntityMgr {

    @Inject
    @Named("dataIntegrationStatusMonitoringReaderRepository")
    private DataIntegrationStatusMonitoringRepository readerRepository;

    @Inject
    @Named("dataIntegrationStatusMonitoringWriterRepository")
    private DataIntegrationStatusMonitoringRepository writerRepository;

    @Inject
    private DataIntegrationStatusMonitoringDao dataIntegrationStatusMonitoringDao;

    @Inject
    private DataIntegrationStatusMonitoringEntityMgrImpl _self;

    @Override
    public BaseDao<DataIntegrationStatusMonitor> getDao() {
        return dataIntegrationStatusMonitoringDao;
    }

    @Override
    protected DataIntegrationStatusMonitoringRepository getReaderRepo() {
        return readerRepository;
    }

    @Override
    protected DataIntegrationStatusMonitoringRepository getWriterRepo() {
        return writerRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<DataIntegrationStatusMonitoringRepository, DataIntegrationStatusMonitor, Long> getSelf() {
        return _self;
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public boolean createStatus(DataIntegrationStatusMonitor status) {
        dataIntegrationStatusMonitoringDao.create(status);
        return true;
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public DataIntegrationStatusMonitor updateStatus(DataIntegrationStatusMonitor status) {
        dataIntegrationStatusMonitoringDao.update(status);
        return status;
    }

    @Transactional(propagation = Propagation.SUPPORTS, readOnly=true)
    @Override
    public DataIntegrationStatusMonitor getStatus(String workflowReqId) {
        return getReaderRepo().findByWorkflowRequestId(workflowReqId);
    }

    @Transactional(propagation = Propagation.SUPPORTS, readOnly=true)
    @Override
    public List<DataIntegrationStatusMonitor> getAllStatuses(Long tenantPid) {
        return getReaderRepo().findAllByTenantPid(tenantPid);
    }

    @Transactional(propagation = Propagation.SUPPORTS, readOnly=true)
    @Override
    public List<DataIntegrationStatusMonitor> getAllStatusesByEntityNameAndIds(Long tenantPid, String entityName, List<String> entityIds) {
        return getReaderRepo().findAllByTenantPidAndEntityNameAndEntityIdIn(tenantPid, entityName, entityIds);
    }
}
