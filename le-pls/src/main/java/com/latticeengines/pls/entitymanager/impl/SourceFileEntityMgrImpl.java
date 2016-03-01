package com.latticeengines.pls.entitymanager.impl;

import org.apache.hadoop.conf.Configuration;
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
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.SecurityContextUtils;

@Component("sourceFileEntityMgr")
public class SourceFileEntityMgrImpl extends BaseEntityMgrImpl<SourceFile> implements SourceFileEntityMgr {

    private static final Logger log = Logger.getLogger(SourceFileEntityMgr.class);

    @Autowired
    private SourceFileDao sourceFileDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private Configuration yarnConfiguration;

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
