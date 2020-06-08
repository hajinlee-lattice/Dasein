package com.latticeengines.apps.cdl.tray.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.tray.dao.TrayConnectorTestDao;
import com.latticeengines.apps.cdl.tray.entitymgr.TrayConnectorTestEntityMgr;
import com.latticeengines.apps.cdl.tray.repository.TrayConnectorTestRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTest;

@Component("trayConnectorTestEntityMgr")
public class TrayConnectorTestEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<TrayConnectorTestRepository, TrayConnectorTest, Long>
        implements TrayConnectorTestEntityMgr {

    @Inject
    private TrayConnectorTestDao trayConnectorTestDao;

    @Inject
    private TrayConnectorTestRepository trayConnectorTestRepository;

    @Override
    public BaseDao<TrayConnectorTest> getDao() {
        return trayConnectorTestDao;
    }

    @Override
    public void create(TrayConnectorTest entity) {
        // TODO Auto-generated method stub

    }

    @Override
    public void deleteByWorkflowRequestId(String workflowRequestId) {
        // TODO Auto-generated method stub

    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public TrayConnectorTest findByWorkflowRequestId(String workflowRequestId) {
        return trayConnectorTestReaderRepository.findByWorkflowRequestId(workflowRequestId);
    }

    @Override
    protected TrayConnectorTestRepository getReaderRepo() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected TrayConnectorTestRepository getWriterRepo() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<TrayConnectorTestRepository, TrayConnectorTest, Long> getSelf() {
        // TODO Auto-generated method stub
        return null;
    }

}
