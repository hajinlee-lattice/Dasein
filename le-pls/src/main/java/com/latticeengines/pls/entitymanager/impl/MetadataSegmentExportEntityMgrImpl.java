package com.latticeengines.pls.entitymanager.impl;

import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.MetadataSegmentExportDao;
import com.latticeengines.pls.entitymanager.MetadataSegmentExportEntityMgr;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("metadataSegmentExportEntityMgr")
public class MetadataSegmentExportEntityMgrImpl extends BaseEntityMgrImpl<MetadataSegmentExport>
        implements MetadataSegmentExportEntityMgr {

    @Autowired
    private MetadataSegmentExportDao metadataSegmentExportDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public BaseDao<MetadataSegmentExport> getDao() {
        return metadataSegmentExportDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false)
    public void create(MetadataSegmentExport entity) {
        Tenant tenant = tenantEntityMgr.findByTenantId(MultiTenantContext.getTenant().getId());
        entity.setTenant(tenant);
        entity.setTenantId(tenant.getPid());
        entity.setExportId(generateExportId());
        metadataSegmentExportDao.create(entity);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<MetadataSegmentExport> findAll() {
        return super.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public MetadataSegmentExport findByExportId(String launchId) {
        return metadataSegmentExportDao.findByExportId(launchId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public void deleteByExportId(String exportId) {
        metadataSegmentExportDao.deleteByExportId(exportId);
    }

    private String generateExportId() {
        return UUID.randomUUID().toString();
    }
}
