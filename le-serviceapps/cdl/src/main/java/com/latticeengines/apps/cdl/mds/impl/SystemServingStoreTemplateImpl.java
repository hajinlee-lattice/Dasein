package com.latticeengines.apps.cdl.mds.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.SystemServingStoreTemplate;
import com.latticeengines.apps.cdl.service.CDLNamespaceService;
import com.latticeengines.apps.core.mds.AMMetadataStore;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datatemplate.DataTemplate;
import com.latticeengines.domain.exposed.metadata.datatemplate.DataTemplateName;
import com.latticeengines.domain.exposed.metadata.datatemplate.DecoratedDataTemplate;
import com.latticeengines.domain.exposed.metadata.mds.DecoratorFactory;
import com.latticeengines.domain.exposed.metadata.mds.MdsDecoratorFactory;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.metadata.DataTemplateProxy;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

@Component
public class SystemServingStoreTemplateImpl extends
        DecoratedDataTemplate<Namespace2<BusinessEntity, DataCollection.Version>, Namespace2<String, String>, Namespace1<String>>
        implements SystemServingStoreTemplate {

    private final CDLNamespaceService cdlNamespaceService;

    @Inject
    public SystemServingStoreTemplateImpl(DataTemplateProxy dataTemplateProxy, AMMetadataStore amMetadataStore,
            CDLNamespaceService cdlNamespaceService) {
        super(getBaseTemplate(dataTemplateProxy), getAMDecorator(amMetadataStore));
        this.cdlNamespaceService = cdlNamespaceService;
    }

    private static DataTemplate<Namespace2<String, String>> getBaseTemplate(DataTemplateProxy proxy) {
        return proxy.toDataTemplate(DataTemplateName.Table, String.class, String.class);
    }

    private static DecoratorFactory<Namespace1<String>> getAMDecorator(AMMetadataStore amMetadataStore) {
        return MdsDecoratorFactory.fromMds("AMDecorator", amMetadataStore);
    }

    @Override
    public Flux<ColumnMetadata> getSchema(Namespace2<BusinessEntity, DataCollection.Version> namespace) {
        return super.getSchema(namespace).map(cm -> { cm.setEntity(namespace.getCoord1()); return cm; });
    }

    @Override
    public ParallelFlux<ColumnMetadata> getUnorderedSchema(
            Namespace2<BusinessEntity, DataCollection.Version> namespace) {
        return super.getUnorderedSchema(namespace).map(cm -> { cm.setEntity(namespace.getCoord1()); return cm; });
    }

    @Override
    protected Namespace2<String, String> projectBaseNamespace(
            Namespace2<BusinessEntity, DataCollection.Version> namespace) {
        return cdlNamespaceService.resolveServingStore(namespace);
    }

    @Override
    protected Namespace1<String> projectDecoratorNamespace(
            Namespace2<BusinessEntity, DataCollection.Version> namespace) {
        return cdlNamespaceService.resolveDataCloudVersion();
    }

}
