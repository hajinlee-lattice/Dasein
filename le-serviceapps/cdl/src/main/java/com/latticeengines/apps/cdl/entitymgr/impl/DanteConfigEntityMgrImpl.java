package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.document.repository.reader.DanteConfigReaderRepository;
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

    @Inject
    private DanteConfigReaderRepository readerRepository;

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
    public List<DanteConfigurationDocument> findAllByTenantId(String tenantId) {
        List<DanteConfigEntity> danteConfigEntities = readerRepository.findByTenantId(tenantId);
        if (danteConfigEntities.size() > 1) {
            log.warn(String.format("Tenant %s have multiple Dante Configurations.", tenantId));
        }
        return danteConfigEntities.stream() //
                .map(DanteConfigEntity::getDocument) //
                .collect(Collectors.toList());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public DanteConfigurationDocument createOrUpdate(String tenantId, DanteConfigurationDocument danteConfig) {

        List<DanteConfigEntity> existing = readerRepository.findByTenantId(tenantId);

        DanteConfigEntity danteConfigEntity = new DanteConfigEntity();
        danteConfigEntity.setUuid(UUID.randomUUID().toString());
        danteConfigEntity.setTenantId(tenantId);
        danteConfigEntity.setDocument(danteConfig);

        if (existing.size() > 0) {
            if (existing.size() > 1) {
                log.warn(String.format("Found multiple Dante Configurations for tenant: %s", tenantId));
            }
            writerRepository.removeByTenantId(tenantId);
        }
        DanteConfigEntity saved = writerRepository.save(danteConfigEntity);
        return saved.getDocument();
    }

}
