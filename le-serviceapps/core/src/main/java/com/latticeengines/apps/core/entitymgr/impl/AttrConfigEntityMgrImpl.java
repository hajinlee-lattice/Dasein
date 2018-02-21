package com.latticeengines.apps.core.entitymgr.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;

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
    public List<AttrConfigEntity> save(String tenantId, BusinessEntity entity, List<AttrConfig> attrConfigs) {
        List<AttrConfigEntity> existing = repository.findByTenantIdAndEntity(tenantId, entity);
        Map<String, AttrConfigEntity> existingMap = new HashMap<>();
        existing.forEach(config -> existingMap.put(config.getMetadata().getAttrName(), config));
        List<AttrConfigEntity> toCreateOrUpdate = new ArrayList<>();
        List<ColumnMetadata> cms = attrConfigs.stream().map(this::toColumnMetadata).collect(Collectors.toList());
        for (ColumnMetadata cm: cms) {
            String attrName = cm.getAttrName();
            AttrConfigEntity attrConfigEntity;
            if (existingMap.containsKey(attrName)) {
                attrConfigEntity = existingMap.get(attrName);
            } else {
                attrConfigEntity = new AttrConfigEntity();
                attrConfigEntity.setUuid(UUID.randomUUID().toString());
                attrConfigEntity.setTenantId(tenantId);
                attrConfigEntity.setEntity(entity);
            }
            attrConfigEntity.setMetadata(cm);
            toCreateOrUpdate.add(attrConfigEntity);
        }
        return repository.saveAll(toCreateOrUpdate);
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
        attrConfig.setAttrName(cm.getAttrName());
        putProperty(attrConfig, ColumnMetadataKey.DisplayName, cm.getDisplayName());
        return attrConfig;
    }

    private ColumnMetadata toColumnMetadata(AttrConfig attrConfig) {
        if (StringUtils.isBlank(attrConfig.getAttrName())) {
            throw new IllegalArgumentException("Must specify attribute name");
        }
        ColumnMetadata cm = new ColumnMetadata();
        cm.setAttrName(attrConfig.getAttrName());
        cm.setDisplayName(getProperty(attrConfig, ColumnMetadataKey.DisplayName, String.class));
        return cm;
    }

    // only persist custom value in this table
    private static <T> T getProperty(AttrConfig attrConfig, String key, Class<T> valueClz) {
        AttrConfigProp prop = attrConfig.getProperty(key);
        if (prop !=null && prop.getCustomValue() != null) {
            return valueClz.cast(prop.getCustomValue());
        }
        return null;
    }

    // only persist custom value in this table
    private static void putProperty(AttrConfig attrConfig, String key, Serializable value) {
        if (value != null) {
            AttrConfigProp prop = new AttrConfigProp();
            prop.setCustomValue(value);
            attrConfig.putProperty(key, prop);
        }
    }

}
