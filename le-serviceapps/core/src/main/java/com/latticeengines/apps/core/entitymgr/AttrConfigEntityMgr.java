package com.latticeengines.apps.core.entitymgr;

import java.util.List;

import com.latticeengines.documentdb.entity.AttrConfigEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface AttrConfigEntityMgr {

    List<AttrConfigEntity> save(String tenantId, BusinessEntity entity, List<AttrConfig> attrConfigs);

    void delete(String tenantId, BusinessEntity entity);

    List<AttrConfig> findAllForEntity(String tenantId, BusinessEntity entity);

    void cleanupTenant(String tenantId);

}
