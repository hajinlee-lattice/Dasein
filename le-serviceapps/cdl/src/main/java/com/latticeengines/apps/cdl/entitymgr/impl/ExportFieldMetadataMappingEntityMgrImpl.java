package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.ExportFieldMetadataMappingDao;
import com.latticeengines.apps.cdl.entitymgr.ExportFieldMetadataMappingEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.repository.ExportFieldMetadataMappingRepository;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataMapping;
import com.latticeengines.domain.exposed.pls.LookupIdMap;

@Component("exportFieldMetadataMappingEntityMgr")
public class ExportFieldMetadataMappingEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<ExportFieldMetadataMappingRepository, ExportFieldMetadataMapping, Long>
        implements ExportFieldMetadataMappingEntityMgr {
    private Logger log = LoggerFactory.getLogger(getClass());

    @Inject
    private ExportFieldMetadataMappingEntityMgrImpl _self;

    @Inject
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

    @Inject
    private ExportFieldMetadataMappingDao exportFieldMappingDao;

    @Resource(name = "exportFieldMetadataMappingWriterRepository")
    private ExportFieldMetadataMappingRepository exportFieldMetadataMappingWriterRepository;

    @Resource(name = "exportFieldMetadataMappingReaderRepository")
    private ExportFieldMetadataMappingRepository exportFieldMetadataMappingReaderRepository;

    @Override
    public ExportFieldMetadataMappingDao getDao() {
        return exportFieldMappingDao;
    }

    @Override
    protected ExportFieldMetadataMappingRepository getReaderRepo() {
        return exportFieldMetadataMappingReaderRepository;
    }

    @Override
    protected ExportFieldMetadataMappingRepository getWriterRepo() {
        return exportFieldMetadataMappingWriterRepository;
    }

    @Override
    protected ExportFieldMetadataMappingEntityMgrImpl getSelf() {
        return _self;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<ExportFieldMetadataMapping> createAll(List<ExportFieldMetadataMapping> exportFieldMappings) {
        log.info(JsonUtils.serialize(exportFieldMappings));
        exportFieldMappingDao.create(exportFieldMappings, true);
        return exportFieldMappings;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
    public List<ExportFieldMetadataMapping> findByOrgId(String orgId) {
        return getReaderRepo().findByOrgId(orgId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<ExportFieldMetadataMapping> update(LookupIdMap lookupIdMap,
            List<ExportFieldMetadataMapping> exportFieldMappings) {
        log.info(JsonUtils.serialize(exportFieldMappings));
        return exportFieldMappingDao.updateExportFieldMetadataMappings(lookupIdMap, exportFieldMappings);
    }

}
