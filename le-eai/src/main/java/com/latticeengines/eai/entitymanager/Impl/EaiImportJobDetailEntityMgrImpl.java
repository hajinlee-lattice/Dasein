package com.latticeengines.eai.entitymanager.Impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.eai.dao.EaiImportJobDetailDao;
import com.latticeengines.eai.entitymanager.EaiImportJobDetailEntityMgr;

@Component("eaiImportJobDetailEntityMgr")
public class EaiImportJobDetailEntityMgrImpl extends BaseEntityMgrImpl<EaiImportJobDetail>
        implements EaiImportJobDetailEntityMgr {

    @Autowired
    private EaiImportJobDetailDao eaiImportJobDetailDao;

    @Override
    public BaseDao<EaiImportJobDetail> getDao() {
        return eaiImportJobDetailDao;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @Override
    public EaiImportJobDetail findByCollectionIdentifier(String identifier) {
        return eaiImportJobDetailDao.findMostRecentRecordByIdentifier(identifier);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @Override
    public EaiImportJobDetail findByApplicationId(String appId) {
        return eaiImportJobDetailDao.findByField("LOAD_APPLICATION_ID", appId);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void createImportJobDetail(EaiImportJobDetail eaiImportJobDetail) {
        EaiImportJobDetail lastDetail = eaiImportJobDetailDao.findMostRecentRecordByIdentifier(eaiImportJobDetail
                .getCollectionIdentifier());
        if (lastDetail == null) {
            eaiImportJobDetail.setSequenceId(1L);
            eaiImportJobDetailDao.create(eaiImportJobDetail);
        } else {
            eaiImportJobDetail.setSequenceId(lastDetail.getSequenceId() + 1);
            eaiImportJobDetailDao.create(eaiImportJobDetail);
        }
    }
}
