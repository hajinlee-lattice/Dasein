package com.latticeengines.apps.cdl.mds.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.CustomizedMetadataStore;
import com.latticeengines.apps.cdl.mds.SystemMetadataStore;
import com.latticeengines.apps.cdl.service.CDLNamespaceService;
import com.latticeengines.apps.core.mds.AttrConfigDecorator;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.mds.DecoratedMetadataStore;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace3;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.StoreFilter;

@Component("customizedMetadataStore")
public class CustomizedMetadataStoreImpl extends
        DecoratedMetadataStore<
                Namespace3<BusinessEntity, DataCollection.Version, StoreFilter>,
                Namespace3<BusinessEntity, DataCollection.Version, StoreFilter>,
                Namespace2<String, BusinessEntity>>
        implements CustomizedMetadataStore {

    private final CDLNamespaceService cdlNamespaceService;

    @Inject
    public CustomizedMetadataStoreImpl(//
            SystemMetadataStore systemMetadataStore, //
            AttrConfigDecorator attrConfigDecorator, //
            CDLNamespaceService cdlNamespaceService) {
        super(systemMetadataStore, attrConfigDecorator);
        this.cdlNamespaceService = cdlNamespaceService;
    }

//    @Override
//    protected Namespace2<String, BusinessEntity> projectDecoratorNamespace(
//            Namespace2<BusinessEntity, DataCollection.Version> namespace) {
//        return cdlNamespaceService.prependTenantId(namespace);
//    }
//
//    @Override
//    protected Namespace3<BusinessEntity, DataCollection.Version, StoreFilter> projectBaseNamespace(
//            Namespace2<BusinessEntity, DataCollection.Version> namespace) {
//        return Namespace.as(namespace.getCoord1(), namespace.getCoord2(), StoreFilter.ALL);
//    }


    @Override
    protected Namespace2<String, BusinessEntity> projectDecoratorNamespace(
            Namespace3<BusinessEntity, DataCollection.Version, StoreFilter> namespace) {
        return cdlNamespaceService.prependTenantId(namespace);
    }

    @Override
    protected Namespace3<BusinessEntity, DataCollection.Version, StoreFilter> projectBaseNamespace(
            Namespace3<BusinessEntity, DataCollection.Version, StoreFilter> namespace) {
        return namespace;
    }
}
