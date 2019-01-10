package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.DataIntegrationStatusMessageDao;
import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMessageEntityMgr;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMessage;

@Component("dataIntegrationStatusMsgEntityMgr")
public class DataIntegrationStatusMessageEntityMgrImpl
        implements DataIntegrationStatusMessageEntityMgr {

    @Inject
    private DataIntegrationStatusMessageDao dataIntegrationStatusMessageDao;

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public void create(DataIntegrationStatusMessage statusMessage) {
        dataIntegrationStatusMessageDao.create(statusMessage);
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public List<DataIntegrationStatusMessage> getAllStatusMessages(String eventId) {
        return dataIntegrationStatusMessageDao.findAllByField("FK_WORKFLOW_REQ_ID", eventId);
    }

}
