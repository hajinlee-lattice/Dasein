package com.latticeengines.apps.cdl.mds.impl;


import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.TableRoleTemplate;
import com.latticeengines.apps.cdl.service.CDLNamespaceService;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datatemplate.DataTemplate;
import com.latticeengines.domain.exposed.metadata.datatemplate.DataTemplateName;
import com.latticeengines.domain.exposed.metadata.datatemplate.DataUnit;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.proxy.exposed.metadata.DataTemplateProxy;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

@Component
public class TableRoleTemplateImpl implements TableRoleTemplate {

    private final CDLNamespaceService cdlNamespaceService;

    private DataTemplate<Namespace2<String, String>> proxyTemplate;

    @Inject
    public TableRoleTemplateImpl(DataTemplateProxy dataTemplateProxy, CDLNamespaceService cdlNamespaceService) {
        this.cdlNamespaceService = cdlNamespaceService;
        this.proxyTemplate = dataTemplateProxy.toDataTemplate(DataTemplateName.Table, String.class, String.class);
    }


    @Override
    public List<DataUnit> getData(Namespace2<TableRoleInCollection, DataCollection.Version> namespace) {
        Namespace2<String, String> servingStoreNs = cdlNamespaceService.resolveTableRole(namespace.getCoord1(), namespace.getCoord2());
        return proxyTemplate.getData(servingStoreNs);
    }

    @Override
    public Flux<ColumnMetadata> getSchema(Namespace2<TableRoleInCollection, DataCollection.Version> namespace) {
        Namespace2<String, String> servingStoreNs = cdlNamespaceService.resolveTableRole(namespace.getCoord1(), namespace.getCoord2());
        return proxyTemplate.getSchema(servingStoreNs);
    }

    @Override
    public ParallelFlux<ColumnMetadata> getUnorderedSchema(Namespace2<TableRoleInCollection, DataCollection.Version> namespace) {
        Namespace2<String, String> servingStoreNs = cdlNamespaceService.resolveTableRole(namespace.getCoord1(), namespace.getCoord2());
        return proxyTemplate.getUnorderedSchema(servingStoreNs);
    }

}
