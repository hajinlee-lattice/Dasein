package com.latticeengines.eai.entitymanager.Impl;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.eai.dao.EaiImportJobDetailDao;
import com.latticeengines.eai.entitymanager.EaiImportJobDetailEntityMgr;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component("eaiImportJobDetailEntityMgr")
public class EaiImportJobDetailEntityMgrImpl extends BaseEntityMgrImpl<EaiImportJobDetail> implements EaiImportJobDetailEntityMgr {

    @Autowired
    private EaiImportJobDetailDao eaiImportJobDetailDao;

    @Override
    public BaseDao<EaiImportJobDetail> getDao() {
        return eaiImportJobDetailDao;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public EaiImportJobDetail findByCollectionIdentifier(String identifier) {
        return eaiImportJobDetailDao.findByField("COLLECTION_IDENTIFIER", identifier);
    }
}

