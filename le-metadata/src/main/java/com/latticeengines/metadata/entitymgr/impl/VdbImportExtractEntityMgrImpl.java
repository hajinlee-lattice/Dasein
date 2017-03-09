package com.latticeengines.metadata.entitymgr.impl;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.VdbImportExtract;
import com.latticeengines.metadata.dao.VdbImportExtractDao;
import com.latticeengines.metadata.entitymgr.VdbImportExtractEntityMgr;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component("vdbImportExtractEntityMgr")
public class VdbImportExtractEntityMgrImpl extends BaseEntityMgrImpl<VdbImportExtract> implements VdbImportExtractEntityMgr {

    @Autowired
    private VdbImportExtractDao vdbImportExtractDao;

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public VdbImportExtract findByExtractIdentifier(String identifier) {
        return vdbImportExtractDao.findByField("EXTRACT_IDENTIFIER", identifier);
    }

    @Override
    public BaseDao<VdbImportExtract> getDao() {
        return vdbImportExtractDao;
    }
}
