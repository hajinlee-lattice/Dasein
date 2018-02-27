package com.latticeengines.apps.cdl.mds.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.CustomizedServingStoreTemplate;
import com.latticeengines.apps.cdl.mds.SystemServingStoreTemplate;
import com.latticeengines.apps.cdl.service.CDLNamespaceService;
import com.latticeengines.apps.core.mds.AttrConfigDecorator;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datatemplate.DecoratedDataTemplate;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component
public class CustomizedServingStoreTemplateImpl extends
        DecoratedDataTemplate<Namespace1<BusinessEntity>, Namespace2<BusinessEntity, DataCollection.Version>, Namespace1<BusinessEntity>>
        implements CustomizedServingStoreTemplate {

    private final CDLNamespaceService cdlNamespaceService;

    @Inject
    public CustomizedServingStoreTemplateImpl( //
            SystemServingStoreTemplate systemTemplate, //
            AttrConfigDecorator attrConfigDecorator, //
            CDLNamespaceService cdlNamespaceService) {
        super(systemTemplate, attrConfigDecorator);
        this.cdlNamespaceService = cdlNamespaceService;
    }

    // (entity) -> (entity)
    @Override
    protected Namespace1<BusinessEntity> projectDecoratorNamespace(Namespace1<BusinessEntity> namespace) {
        return namespace;
    }

    // (entity) -> (entity, version)
    @Override
    protected Namespace2<BusinessEntity, DataCollection.Version> projectBaseNamespace(
            Namespace1<BusinessEntity> namespace) {
        return cdlNamespaceService.appendActiveVersion(namespace);
    }

}
