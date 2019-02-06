package com.latticeengines.pls.entitymanager.impl;

import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport.Status;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.MetadataSegmentExportDao;
import com.latticeengines.pls.entitymanager.MetadataSegmentExportEntityMgr;

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
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(MetadataSegmentExport entity) {
        Tenant tenant = tenantEntityMgr.findByTenantId(MultiTenantContext.getTenant().getId());
        CustomerSpace customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId());
        String path = PathBuilder.buildDataFileUniqueExportPath(CamilleEnvironment.getPodId(), customerSpace)
                .toString();
        entity.setPath(path);

        entity.setTenant(tenant);
        entity.setTenantId(tenant.getPid());
        entity.setExportId(generateExportId());
        entity.setStatus(Status.RUNNING);
        entity.setCleanupBy(new Date(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(7)));

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
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteByExportId(String exportId) {
        metadataSegmentExportDao.deleteByExportId(exportId);
    }

    private String generateExportId() {
        return UUID.randomUUID().toString();
    }
}
