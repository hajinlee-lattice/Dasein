package com.latticeengines.apps.core.mds.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.mds.AttrConfigDecorator;
import com.latticeengines.apps.core.service.NamespaceService;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.mds.DecoratorFactory;
import com.latticeengines.domain.exposed.metadata.mds.MdsDecoratorFactory;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStore;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStoreName;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.metadata.MetadataStoreProxy;

@Component
public class AttrConfigDecoratorImpl implements AttrConfigDecorator {

    private final NamespaceService commonNamespaceService;
    private final DecoratorFactory<Namespace2<String, BusinessEntity>> kernel;

    @Inject
    public AttrConfigDecoratorImpl(MetadataStoreProxy metadataStoreProxy, NamespaceService commonNamespaceService) {
        this.kernel = getFactory(metadataStoreProxy);
        this.commonNamespaceService = commonNamespaceService;
    }

    private static DecoratorFactory<Namespace2<String, BusinessEntity>> getFactory(
            MetadataStoreProxy metadataStoreProxy) {
        MetadataStore<Namespace2<String, BusinessEntity>> mds = metadataStoreProxy
                .toMetadataStore(MetadataStoreName.AttrConfig, String.class, BusinessEntity.class);
        return MdsDecoratorFactory.fromMds("AttrConfigDecorator", mds);
    }

    @Override
    public Decorator getDecorator(Namespace1<BusinessEntity> namespace) {
        return kernel.getDecorator(commonNamespaceService.prefixTenantId(namespace));
    }

}
