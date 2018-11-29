package com.latticeengines.apps.core.entitymgr;

import java.util.List;

import com.latticeengines.documentdb.entity.AttrConfigEntity;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;

public interface AttrConfigEntityMgr {

    List<AttrConfig> save(String tenantId, BusinessEntity entity, List<AttrConfig> attrConfigs);

    void deleteAllForEntity(String tenantId, BusinessEntity entity);

    List<AttrConfig> findAllForEntity(String tenantId, BusinessEntity entity);

    List<AttrConfigEntity> findAllByTenantAndEntity(String tenantId, BusinessEntity entity);

    List<AttrConfig> findAllForEntityInReader(String tenantId, BusinessEntity entity);

    void cleanupTenant(String tenantId);

    void deleteConfigs(List<AttrConfigEntity> entities);

    List<AttrConfig> findAllByTenantId(String tenantId);

    List<AttrConfig> findAllHaveCustomDisplayNameByTenantId(String tenantId);

    void deleteByAttrNameStartingWith(String attrName);
}
