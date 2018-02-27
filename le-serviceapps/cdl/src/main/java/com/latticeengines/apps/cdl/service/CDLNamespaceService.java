package com.latticeengines.apps.cdl.service;

import java.io.Serializable;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface CDLNamespaceService {

    Namespace2<String, String> resolveTableRole(TableRoleInCollection role, DataCollection.Version version);

    Namespace2<String, String> resolveServingStore(BusinessEntity businessEntity, DataCollection.Version version);
    Namespace2<String, String> resolveServingStore(Namespace2<BusinessEntity, DataCollection.Version> namespace);

    Namespace1<String> resolveDataCloudVersion();

    <T extends Serializable> Namespace2<T, DataCollection.Version> appendActiveVersion(T coord);
    <T extends Serializable> Namespace2<T, DataCollection.Version> appendActiveVersion(Namespace1<T> namespace);

}
