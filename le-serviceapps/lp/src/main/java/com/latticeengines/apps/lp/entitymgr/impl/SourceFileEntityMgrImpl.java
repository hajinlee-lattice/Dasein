package com.latticeengines.apps.lp.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.lp.dao.SourceFileDao;
import com.latticeengines.apps.lp.entitymgr.SourceFileEntityMgr;
import com.latticeengines.apps.lp.repository.writer.SourceFileWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;

@Component
public class SourceFileEntityMgrImpl extends BaseEntityMgrRepositoryImpl<SourceFile, Long>
        implements SourceFileEntityMgr {

    @Inject
    private SourceFileDao dao;

    @Inject
    private SourceFileWriterRepository repository;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public BaseJpaRepository<SourceFile, Long> getRepository() {
        return repository;
    }

    @Override
    public BaseDao<SourceFile> getDao() {
        return dao;
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
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(SourceFile sourceFile, Tenant tenant) {
        sourceFile.setTenant(tenant);
        sourceFile.setTenantId(tenant.getPid());
        sourceFile.setPid(null);
        super.create(sourceFile);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public SourceFile findByName(String name) {
        return dao.findByName(name);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public SourceFile findByNameFromWriter(String name) {
        return dao.findByName(name);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public SourceFile findByApplicationId(String applicationId) {
        return dao.findByApplicationId(applicationId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public SourceFile findByWorkflowPid(Long workflowPid) {
        return dao.findByWorkflowPid(workflowPid);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public SourceFile findByTableName(String tableName) {
        return dao.findByTableName(tableName);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public SourceFile getByTableName(String tableName) {
        return dao.getByTableName(tableName);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<SourceFile> findAllSourceFiles() {
        return dao.findAllSourceFiles();
    }


}
