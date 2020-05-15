package com.latticeengines.apps.core.mds.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.apps.core.mds.AttrConfigDecorator;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.mds.MapDecorator;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;

@Component
public class AttrConfigDecoratorImpl implements AttrConfigDecorator {

    private final AttrConfigEntityMgr entityMgr;

    @Inject
    public AttrConfigDecoratorImpl(AttrConfigEntityMgr attrConfigEntityMgr) {
        this.entityMgr = attrConfigEntityMgr;
    }

    @Override
    public Decorator getDecorator(Namespace2<String, BusinessEntity> namespace) {
        final String tenantId = CustomerSpace.shortenCustomerSpace(namespace.getCoord1());
        final Tenant tenant = MultiTenantContext.getTenant();
        final BusinessEntity entity = namespace.getCoord2();
        return new MapDecorator("AttrConfig") {
            @Override
            protected Collection<ColumnMetadata> loadInternal() {
                MultiTenantContext.setTenant(tenant);
                List<AttrConfig> attrConfigList = entityMgr.findAllForEntityInReader(tenantId, entity);
                if (CollectionUtils.isNotEmpty(attrConfigList)) {
                    return attrConfigList.stream().map(AttrConfig::toColumnMetadata).collect(Collectors.toList());
                } else {
                    return Collections.emptyList();
                }
            }
        };
    }

}
