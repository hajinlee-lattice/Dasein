package com.latticeengines.apps.core.service;

import java.io.Serializable;

import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace3;

public interface NamespaceService {

    <T extends Serializable> Namespace2<String, T> prefixTenantId(Namespace1<T> namespace);
    <T1 extends Serializable, T2 extends Serializable> Namespace3<String, T1, T2> prefixTenantId(Namespace2<T1, T2> namespace);

}
