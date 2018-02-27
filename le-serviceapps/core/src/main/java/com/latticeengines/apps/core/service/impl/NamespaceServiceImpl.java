package com.latticeengines.apps.core.service.impl;

import java.io.Serializable;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.core.service.NamespaceService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace3;

@Service("namespaceService")
public class NamespaceServiceImpl implements NamespaceService {

    @Override
    public <T extends Serializable> Namespace2<String, T> prefixTenantId(Namespace1<T> namespace) {
        String tenantId = MultiTenantContext.getTenantId();
        return Namespace.as(tenantId, namespace.getCoord1());
    }

    @Override
    public <T1 extends Serializable, T2 extends Serializable> Namespace3<String, T1, T2> prefixTenantId(
            Namespace2<T1, T2> namespace) {
        String tenantId = MultiTenantContext.getTenantId();
        return Namespace.as(tenantId, namespace.getCoord1(), namespace.getCoord2());
    }

}
