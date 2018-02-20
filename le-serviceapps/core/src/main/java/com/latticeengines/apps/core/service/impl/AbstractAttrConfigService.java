package com.latticeengines.apps.core.service.impl;

import java.util.List;

import javax.inject.Inject;

import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;

public abstract class AbstractAttrConfigService {

    @Inject
    private AttrConfigEntityMgr entityMgr;

    protected abstract List<ColumnMetadata> getSystemMetadata(BusinessEntity entity);

    protected List<AttrConfig> getCustomConfig(BusinessEntity entity) {
        String tenantId = MultiTenantContext.getTenantId();
        return entityMgr.findAllForEntity(tenantId, entity);
    }

}
