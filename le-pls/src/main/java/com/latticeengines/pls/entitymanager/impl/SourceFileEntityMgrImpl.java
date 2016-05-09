package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.SourceFileDao;
import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("sourceFileEntityMgr")
public class SourceFileEntityMgrImpl extends BaseEntityMgrImpl<SourceFile> implements SourceFileEntityMgr {

    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(SourceFileEntityMgr.class);

    @Autowired
    private SourceFileDao sourceFileDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public BaseDao<SourceFile> getDao() {
        return sourceFileDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(SourceFile sourceFile) {
        Tenant tenant = tenantEntityMgr.findByTenantId(MultiTenantContext.getTenant().getId());
        sourceFile.setTenant(tenant);
        sourceFile.setTenantId(tenant.getPid());
        sourceFile.setPid(null);
        super.create(sourceFile);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public SourceFile findByName(String name) {
        return sourceFileDao.findByName(name);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public SourceFile findByApplicationId(String applicationId) {
        return sourceFileDao.findByApplicationId(applicationId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<SourceFile> findAllSourceFiles() {
        return sourceFileDao.findAllSourceFiles();
    }
}
