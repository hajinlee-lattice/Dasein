package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.document.repository.writer.DanteConfigWriterRepository;
import com.latticeengines.apps.cdl.entitymgr.DanteConfigEntityMgr;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.documentdb.entity.DanteConfigEntity;
import com.latticeengines.documentdb.entitymgr.impl.BaseDocumentEntityMgrImpl;
import com.latticeengines.domain.exposed.dante.DanteConfigurationDocument;

@Component("danteConfigEntityMgr")
public class DanteConfigEntityMgrImpl extends BaseDocumentEntityMgrImpl<DanteConfigEntity>
        implements DanteConfigEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(DanteConfigEntityMgrImpl.class);

    @Inject
    private DanteConfigWriterRepository writerRepository;

    @Override
    public BaseJpaRepository<DanteConfigEntity, String> getRepository() {
        return writerRepository;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteByTenantId(String tenantId) {
        List<DanteConfigEntity> entities = writerRepository.removeByTenantId(tenantId);
        log.info("Deleted " + entities.size() + " documents from " + tenantId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DanteConfigurationDocument findByTenantId(String tenantId) {
        DanteConfigEntity danteConfigEntity = writerRepository.findByTenantId(tenantId);
        if (danteConfigEntity != null) {
            return danteConfigEntity.getDocument();
        }
        return null;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public DanteConfigurationDocument createOrUpdate(String tenantId, DanteConfigurationDocument danteConfig) {

        DanteConfigEntity existing = writerRepository.findByTenantId(tenantId);

        DanteConfigEntity danteConfigEntity = new DanteConfigEntity();
        danteConfigEntity.setUuid(UUID.randomUUID().toString());
        danteConfigEntity.setTenantId(tenantId);
        danteConfigEntity.setDocument(danteConfig);

        if (existing != null) {
            writerRepository.updateTenantDocument(tenantId, danteConfig);
            return writerRepository.findByTenantId(tenantId).getDocument();
        }

        DanteConfigEntity saved = writerRepository.save(danteConfigEntity);
        return saved.getDocument();
    }

}
