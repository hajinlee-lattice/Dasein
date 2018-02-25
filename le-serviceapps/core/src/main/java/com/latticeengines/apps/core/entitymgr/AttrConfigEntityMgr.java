package com.latticeengines.apps.core.entitymgr;

import java.util.List;

import com.latticeengines.documentdb.entity.AttrConfigEntity;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;

public interface AttrConfigEntityMgr {

    // this will remove existing entities not in the input list
    List<AttrConfigEntity> reset(String tenantId, BusinessEntity entity, List<AttrConfig> attrConfigs);

    // this will not remove existing entities not in the input list
    List<AttrConfigEntity> upsert(String tenantId, BusinessEntity entity, List<AttrConfig> attrConfigs);

    void delete(String tenantId, BusinessEntity entity);

    List<AttrConfig> findAllForEntity(String tenantId, BusinessEntity entity);

    void cleanupTenant(String tenantId);

}
