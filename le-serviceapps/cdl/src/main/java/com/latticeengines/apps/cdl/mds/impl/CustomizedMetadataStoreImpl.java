package com.latticeengines.apps.cdl.mds.impl;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.AttributeSetDecoratorFac;
import com.latticeengines.apps.cdl.mds.CustomizedMetadataStore;
import com.latticeengines.apps.cdl.mds.SystemMetadataStore;
import com.latticeengines.apps.cdl.service.CDLNamespaceService;
import com.latticeengines.apps.core.mds.AttrConfigDecorator;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.mds.ChainedDecoratorFactory;
import com.latticeengines.domain.exposed.metadata.mds.DecoratedMetadataStore;
import com.latticeengines.domain.exposed.metadata.mds.DecoratorFactory;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace3;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.StoreFilter;

@Component("customizedMetadataStore")
public class CustomizedMetadataStoreImpl extends
        DecoratedMetadataStore<Namespace3<BusinessEntity, DataCollection.Version, StoreFilter>,
                Namespace3<BusinessEntity, DataCollection.Version, StoreFilter>,
                Namespace2<String, BusinessEntity>>
        implements CustomizedMetadataStore {

    private final CDLNamespaceService cdlNamespaceService;

    @Inject
    public CustomizedMetadataStoreImpl(
            SystemMetadataStore systemMetadataStore,
            AttrConfigDecorator attrConfigDecorator,
            AttributeSetDecoratorFac attributeSetDecoratorFac,
            CDLNamespaceService cdlNamespaceService) {
        super(systemMetadataStore, getDecoratorChain(attrConfigDecorator, attributeSetDecoratorFac));
        this.cdlNamespaceService = cdlNamespaceService;
    }

    private static ChainedDecoratorFactory<Namespace2<String, BusinessEntity>> getDecoratorChain(
            AttrConfigDecorator attrConfigDecorator, AttributeSetDecoratorFac attributeSetDecoratorFac) {
        List<DecoratorFactory<? extends Namespace>> factories = Arrays.asList(attrConfigDecorator, attributeSetDecoratorFac);
        return new ChainedDecoratorFactory<Namespace2<String, BusinessEntity>>("CustomServingStoreChain", factories) {
            @Override
            protected List<Namespace> project(Namespace2<String, BusinessEntity> namespace) {
                return Arrays.asList(namespace, namespace);
            }
        };
    }

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
