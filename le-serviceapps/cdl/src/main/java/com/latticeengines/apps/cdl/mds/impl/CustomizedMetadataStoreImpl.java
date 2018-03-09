package com.latticeengines.apps.cdl.mds.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.CustomizedMetadataStore;
import com.latticeengines.apps.cdl.mds.SystemMetadataStore;
import com.latticeengines.apps.cdl.service.CDLNamespaceService;
import com.latticeengines.apps.core.mds.AttrConfigDecorator;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.mds.DecoratedMetadataStore;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component
public class CustomizedMetadataStoreImpl extends
        DecoratedMetadataStore<Namespace1<BusinessEntity>, Namespace2<BusinessEntity, DataCollection.Version>, Namespace2<String, BusinessEntity>>
        implements CustomizedMetadataStore {

    private final CDLNamespaceService cdlNamespaceService;

    @Inject
    public CustomizedMetadataStoreImpl( //
                                        SystemMetadataStore systemMetadataStore, //
                                        AttrConfigDecorator attrConfigDecorator, //
                                        CDLNamespaceService cdlNamespaceService) {
        super(systemMetadataStore, attrConfigDecorator);
        this.cdlNamespaceService = cdlNamespaceService;
    }

    // (entity) -> (tenantId, entity)
    @Override
    protected Namespace2<String, BusinessEntity> projectDecoratorNamespace(Namespace1<BusinessEntity> namespace) {
        return cdlNamespaceService.prependTenantId(namespace);
    }

    // (entity) -> (entity, version)
    @Override
    protected Namespace2<BusinessEntity, DataCollection.Version> projectBaseNamespace(
            Namespace1<BusinessEntity> namespace) {
        return cdlNamespaceService.appendActiveVersion(namespace);
    }

}
