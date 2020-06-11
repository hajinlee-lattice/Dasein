package com.latticeengines.apps.cdl.tray.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.tray.dao.TrayConnectorTestDao;
import com.latticeengines.apps.cdl.tray.entitymgr.TrayConnectorTestEntityMgr;
import com.latticeengines.apps.cdl.tray.repository.TrayConnectorTestRepository;
import com.latticeengines.apps.cdl.tray.repository.reader.TrayConnectorTestReaderRepository;
import com.latticeengines.apps.cdl.tray.repository.writer.TrayConnectorTestWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTest;

@Component("trayConnectorTestEntityMgr")
public class TrayConnectorTestEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<TrayConnectorTestRepository, TrayConnectorTest, Long>
        implements TrayConnectorTestEntityMgr {

    @Inject
    private TrayConnectorTestEntityMgrImpl _self;

    @Inject
    private TrayConnectorTestDao trayConnectorTestDao;

    @Inject
    private TrayConnectorTestReaderRepository trayConnectorTestReaderRepository;

    @Inject
    private TrayConnectorTestWriterRepository trayConnectorTestWriterRepository;

    @Override
    public BaseDao<TrayConnectorTest> getDao() {
        return trayConnectorTestDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(TrayConnectorTest trayConnectorTest) {
        trayConnectorTest.setTenant(MultiTenantContext.getTenant());
        super.create(trayConnectorTest);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteByWorkflowRequestId(String workflowRequestId) {
        TrayConnectorTest trayConnectorTest = trayConnectorTestWriterRepository
                .findByWorkflowRequestId(workflowRequestId);
        if (trayConnectorTest != null) {
            delete(trayConnectorTest);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public TrayConnectorTest findByWorkflowRequestId(String workflowRequestId) {
        return trayConnectorTestReaderRepository.findByWorkflowRequestId(workflowRequestId);
    }

    @Override
    protected TrayConnectorTestRepository getReaderRepo() {
        return trayConnectorTestReaderRepository;
    }

    @Override
    protected TrayConnectorTestRepository getWriterRepo() {
        return trayConnectorTestWriterRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<TrayConnectorTestRepository, TrayConnectorTest, Long> getSelf() {
        return _self;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public TrayConnectorTest updateTrayConnectorTest(TrayConnectorTest trayConnectorTest) {
        TrayConnectorTest existingTest = findByWorkflowRequestId(trayConnectorTest.getWorkflowRequestId());
        if (existingTest != null) {
            mergeTrayConnectorTest(existingTest, trayConnectorTest);
            trayConnectorTestDao.update(existingTest);
            return existingTest;
        } else {
            throw new RuntimeException(
                    String.format("TrayConnectorTest %s does not exist.", trayConnectorTest.getWorkflowRequestId()));
        }
    }

    private void mergeTrayConnectorTest(TrayConnectorTest oldTest, TrayConnectorTest newTest) {
        if (newTest.getEndTime() != null) {
            oldTest.setEndTime(newTest.getEndTime());
        }
        if (newTest.getErrorDetails() != null) {
            oldTest.setErrorDetails(newTest.getErrorDetails());
        }
        if (newTest.getTestState() != null) {
            oldTest.setTestState(newTest.getTestState());
        }
    }

}
