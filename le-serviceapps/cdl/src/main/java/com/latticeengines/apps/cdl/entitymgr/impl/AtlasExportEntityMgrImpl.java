package com.latticeengines.apps.cdl.entitymgr.impl;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.AtlasExportDao;
import com.latticeengines.apps.cdl.entitymgr.AtlasExportEntityMgr;
import com.latticeengines.apps.cdl.repository.AtlasExportRepository;
import com.latticeengines.apps.cdl.repository.reader.AtlasExportReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.AtlasExportWriterRepository;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("atlasExportEntityMgr")
public class AtlasExportEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<AtlasExportRepository, AtlasExport, Long>
        implements AtlasExportEntityMgr {

    private static final String UUID_PREFIX = "AtlasExport";

    private static final SimpleDateFormat exportDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    @Inject
    private AtlasExportEntityMgrImpl _self;

    @Inject
    private AtlasExportDao atlasExportDao;

    @Inject
    private AtlasExportReaderRepository readerRepository;

    @Inject
    private AtlasExportWriterRepository writerRepository;

    @Override
    protected AtlasExportRepository getReaderRepo() {
        return readerRepository;
    }

    @Override
    protected AtlasExportRepository getWriterRepo() {
        return writerRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<AtlasExportRepository, AtlasExport, Long> getSelf() {
        return _self;
    }

    @Override
    public BaseDao<AtlasExport> getDao() {
        return atlasExportDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public AtlasExport findByUuid(String uuid) {
        if (isReaderConnection()) {
            return readerRepository.findByUuid(uuid);
        } else {
            return writerRepository.findByUuid(uuid);
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public AtlasExport createAtlasExport(AtlasExportType exportType) {
        Tenant tenant = MultiTenantContext.getTenant();
        AtlasExport export = new AtlasExport();
        export.setTenant(tenant);
        export.setExportType(exportType);
        export.setStatus(MetadataSegmentExport.Status.RUNNING);
        export.setUuid(NamingUtils.uuid(UUID_PREFIX));
        export.setScheduled(true);
        export.setDatePrefix(exportDateFormat.format(new Date()));
        export.setCleanupBy(new Date(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(30)));
        atlasExportDao.create(export);
        return export;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public AtlasExport createAtlasExport(AtlasExport atlasExport) {
        Tenant tenant = MultiTenantContext.getTenant();
        atlasExport.setTenant(tenant);
        atlasExport.setTenantId(tenant.getPid());
        atlasExport.setUuid(NamingUtils.uuid(UUID_PREFIX));
        atlasExport.setStatus(MetadataSegmentExport.Status.RUNNING);
        atlasExport.setDatePrefix(exportDateFormat.format(new Date()));
        atlasExport.setCleanupBy(new Date(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(7)));
        atlasExportDao.create(atlasExport);
        return atlasExport;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void deleteByExportId(String exportId) {
        AtlasExport atlasExport = findByUuid(exportId);
        if (atlasExport != null) {
            atlasExportDao.delete(atlasExport);
        }
    }

}
