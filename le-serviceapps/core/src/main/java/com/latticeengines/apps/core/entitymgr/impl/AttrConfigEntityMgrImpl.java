package com.latticeengines.apps.core.entitymgr.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.core.document.repository.reader.AttrConfigEntityReaderRepository;
import com.latticeengines.apps.core.document.repository.writer.AttrConfigEntityRepository;
import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.documentdb.entity.AttrConfigEntity;
import com.latticeengines.documentdb.entitymgr.impl.BaseDocumentEntityMgrImpl;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;

@Component("attrConfigEntityMgr")
public class AttrConfigEntityMgrImpl extends BaseDocumentEntityMgrImpl<AttrConfigEntity>
        implements AttrConfigEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(AttrConfigEntityMgrImpl.class);

    @Inject
    private AttrConfigEntityRepository repository;

    @Inject
    private AttrConfigEntityReaderRepository readerRepository;

    @Override
    public BaseJpaRepository<AttrConfigEntity, String> getRepository() {
        return repository;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public List<AttrConfig> save(String tenantId, BusinessEntity entity, List<AttrConfig> attrConfigs) {
        List<AttrConfigEntity> existing = repository.findByTenantIdAndEntity(tenantId, entity);
        Map<String, AttrConfigEntity> existingMap = new HashMap<>();
        existing.forEach(config -> existingMap.put(config.getDocument().getAttrName(), config));
        List<AttrConfigEntity> toCreateOrUpdate = new ArrayList<>();
        for (AttrConfig attrConfig : attrConfigs) {
            String attrName = attrConfig.getAttrName();
            AttrConfigEntity attrConfigEntity;
            if (existingMap.containsKey(attrName)) {
                attrConfigEntity = existingMap.get(attrName);
            } else {
                attrConfigEntity = new AttrConfigEntity();
                attrConfigEntity.setUuid(UUID.randomUUID().toString());
                attrConfigEntity.setTenantId(tenantId);
            }
            attrConfig.setEntity(entity);
            attrConfigEntity.setDocument(attrConfig);
            toCreateOrUpdate.add(attrConfigEntity);
        }
        List<AttrConfigEntity> saved = repository.saveAll(toCreateOrUpdate);
        return saved.stream().map(AttrConfigEntity::getDocument).collect(Collectors.toList());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteAllForEntity(String tenantId, BusinessEntity entity) {
        repository.removeByTenantIdAndEntity(tenantId, entity);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AttrConfig> findAllForEntity(String tenantId, BusinessEntity entity) {
        List<AttrConfigEntity> attrConfigEntities = repository.findByTenantIdAndEntity(tenantId, entity);
        return attrConfigEntities.stream() //
                .map(AttrConfigEntity::getDocument) //
                .collect(Collectors.toList());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AttrConfig> findAllForEntityInReader(String tenantId, BusinessEntity entity) {
        List<AttrConfigEntity> attrConfigEntities = readerRepository.findByTenantIdAndEntity(tenantId, entity);
        return attrConfigEntities.stream() //
                .map(AttrConfigEntity::getDocument) //
                .collect(Collectors.toList());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AttrConfig> findAllInEntitiesInReader(String tenantId, List<BusinessEntity> entities) {
        List<AttrConfigEntity> attrConfigEntities = readerRepository.findByTenantIdAndEntityIn(tenantId, entities);
        return attrConfigEntities.stream() //
                .map(AttrConfigEntity::getDocument) //
                .collect(Collectors.toList());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void cleanupTenant(String tenantId) {
        List<AttrConfigEntity> entities = repository.removeByTenantId(tenantId);
        log.info("Deleted " + entities.size() + " documents from " + tenantId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AttrConfig> findAllByTenantId(String tenantId) {
        List<AttrConfigEntity> attrConfigEntities = repository.findByTenantId(tenantId);
        return attrConfigEntities.stream() //
                .map(AttrConfigEntity::getDocument) //
                .collect(Collectors.toList());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AttrConfig> findAllHaveCustomDisplayNameByTenantId(String tenantId) {
        List<AttrConfigEntity> attrConfigEntities = repository.findAllHaveCustomDisplayNameByTenantId(tenantId);
        return attrConfigEntities.stream() //
                .map(AttrConfigEntity::getDocument) //
                .collect(Collectors.toList());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteConfigs(List<AttrConfigEntity> entities) {
        repository.deleteInBatch(entities);
        log.info("Deleted " + entities.size() + "");
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AttrConfigEntity> findAllByTenantAndEntity(String tenantId, BusinessEntity entity) {
        return repository.findByTenantIdAndEntity(tenantId, entity);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteByAttrNameStartingWith(String attrName) {
        List<AttrConfigEntity> entities = repository.removeByAttrNameStartingWith(attrName);
        log.info("Deleted " + entities.size() + " documents whose name like " + attrName);
    }
}
