package com.latticeengines.workflow.exposed.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.SecurityContextUtils;
import com.latticeengines.workflow.exposed.dao.SourceFileDao;
import com.latticeengines.workflow.exposed.entitymgr.SourceFileEntityMgr;

@Component("sourceFileEntityMgr")
public class SourceFileEntityMgrImpl extends BaseEntityMgrImpl<SourceFile> implements SourceFileEntityMgr {

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
        Tenant tenant = tenantEntityMgr.findByTenantId(SecurityContextUtils.getTenant().getId());
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

}
