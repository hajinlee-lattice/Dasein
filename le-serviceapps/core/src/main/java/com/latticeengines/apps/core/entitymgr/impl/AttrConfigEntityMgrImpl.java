package com.latticeengines.apps.core.entitymgr.impl;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.core.document.repository.AttrConfigEntityRepository;
import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.documentdb.entity.AttrConfigEntity;
import com.latticeengines.documentdb.entitymgr.impl.BaseDocumentEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;

@Component("attrConfigEntityMgr")
public class AttrConfigEntityMgrImpl extends BaseDocumentEntityMgrImpl<AttrConfigEntity> implements AttrConfigEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(AttrConfigEntityMgrImpl.class);

    @Inject
    private AttrConfigEntityRepository repository;

    @Override
    public BaseJpaRepository<AttrConfigEntity, String> getRepository() {
        return repository;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void save(String tenantId, BusinessEntity entity, List<AttrConfig> attrConfigs) {
        repository.removeByTenantIdAndEntity(tenantId, entity);
        List<ColumnMetadata> cms = attrConfigs.stream().map(this::toColumnMetadata).collect(Collectors.toList());
        for (ColumnMetadata cm: cms) {
            AttrConfigEntity attrConfigEntity = new AttrConfigEntity();
            attrConfigEntity.setTenantId(tenantId);
            attrConfigEntity.setEntity(entity);
            attrConfigEntity.setUuid(UUID.randomUUID().toString());
            attrConfigEntity.setMetadata(cm);
            repository.save(attrConfigEntity);
        }
    }


    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void delete(String tenantId, BusinessEntity entity) {
        repository.removeByTenantIdAndEntity(tenantId, entity);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AttrConfig> findAllForEntity(String tenantId, BusinessEntity entity) {
        List<AttrConfigEntity> attrConfigEntities = repository.findByTenantIdAndEntity(tenantId, entity);
        return attrConfigEntities.stream() //
                .map(AttrConfigEntity::getMetadata) //
                .map(this::toAttrConfig) //
                .collect(Collectors.toList());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void cleanupTenant(String tenantId) {
        List<AttrConfigEntity> entities = repository.removeByTenantId(tenantId);
        log.info("Deleted " + entities.size() + " documents from " + tenantId);
    }

    private AttrConfig toAttrConfig(ColumnMetadata cm) {
        AttrConfig attrConfig = new AttrConfig();

        attrConfig.setAttrName(cm.getColumnId());

        return attrConfig;
    }

    private ColumnMetadata toColumnMetadata(AttrConfig attrConfig) {
        if (StringUtils.isBlank(attrConfig.getAttrName())) {
            throw new IllegalArgumentException("Must specify attribute name");
        }
        ColumnMetadata cm = new ColumnMetadata();

        cm.setColumnId(attrConfig.getAttrName());

        return cm;
    }

}
