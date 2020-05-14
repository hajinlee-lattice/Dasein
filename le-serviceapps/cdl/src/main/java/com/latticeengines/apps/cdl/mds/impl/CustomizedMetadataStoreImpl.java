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
import com.latticeengines.domain.exposed.metadata.namespace.Namespace3;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace4;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.StoreFilter;

@Component("customizedMetadataStore")
public class CustomizedMetadataStoreImpl extends
        DecoratedMetadataStore<Namespace4<BusinessEntity, DataCollection.Version, StoreFilter, String>,
                Namespace3<BusinessEntity, DataCollection.Version, StoreFilter>,
                Namespace3<String, BusinessEntity, String>>
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

    private static ChainedDecoratorFactory<Namespace3<String, BusinessEntity, String>> getDecoratorChain(
            AttrConfigDecorator attrConfigDecorator, AttributeSetDecoratorFac attributeSetDecoratorFac) {
        List<DecoratorFactory<? extends Namespace>> factories = Arrays.asList(attrConfigDecorator, attributeSetDecoratorFac);
        return new ChainedDecoratorFactory<Namespace3<String, BusinessEntity, String>>("CustomServingStoreChain",
                factories) {
            @Override
            protected List<Namespace> project(Namespace3<String, BusinessEntity, String> namespace) {
                return Arrays.asList(namespace, namespace);
            }
        };
    }

    @Override
    protected Namespace3<String, BusinessEntity, String> projectDecoratorNamespace(
            Namespace4<BusinessEntity, DataCollection.Version, StoreFilter, String> namespace) {
        return cdlNamespaceService.prependTenantIdAndAttributeSet(Namespace.as(namespace.getCoord1(), namespace.getCoord4()));
    }

    @Override
    protected Namespace3<BusinessEntity, DataCollection.Version, StoreFilter> projectBaseNamespace(
            Namespace4<BusinessEntity, DataCollection.Version, StoreFilter, String> namespace) {
        return namespace;
    }
}
