package com.latticeengines.metadata.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.metadata.dao.ModuleDao;
import com.latticeengines.metadata.entitymgr.ModuleEntityMgr;

@Component("moduleEntityMgr")
public class ModuleEntityMgrImpl extends BaseEntityMgrImpl<Module> implements ModuleEntityMgr {

    @Inject
    private ModuleDao moduleDao;

    @Override
    public BaseDao<Module> getDao() {
        return moduleDao;
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public Module findByName(String name) {
        return moduleDao.findByField("NAME", name);
    }

}
