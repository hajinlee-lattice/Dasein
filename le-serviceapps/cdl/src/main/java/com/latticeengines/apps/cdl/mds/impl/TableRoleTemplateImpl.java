package com.latticeengines.apps.cdl.mds.impl;


import java.util.List;

import javax.inject.Inject;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.TableRoleTemplate;
import com.latticeengines.apps.cdl.service.CDLNamespaceService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cache.CacheName;
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

@Component("tableRoleTemplate")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class TableRoleTemplateImpl implements TableRoleTemplate {

    private final CDLNamespaceService cdlNamespaceService;
    private final TableRoleTemplate _tableRoleTemplate;

    private DataTemplate<Namespace2<String, String>> proxyTemplate;

    @Inject
    public TableRoleTemplateImpl(DataTemplateProxy dataTemplateProxy, CDLNamespaceService cdlNamespaceService, TableRoleTemplate tableRoleTemplate) {
        this.cdlNamespaceService = cdlNamespaceService;
        this.proxyTemplate = dataTemplateProxy.toDataTemplate(DataTemplateName.Table, String.class, String.class);
        this._tableRoleTemplate = tableRoleTemplate;
    }

    @Override
    public List<DataUnit> getData(Namespace2<TableRoleInCollection, DataCollection.Version> namespace) {
        Namespace2<String, String> servingStoreNs = cdlNamespaceService.resolveTableRole(namespace.getCoord1(), namespace.getCoord2());
        return proxyTemplate.getData(servingStoreNs);
    }

    @Override
    public Flux<ColumnMetadata> getSchema(Namespace2<TableRoleInCollection, DataCollection.Version> namespace) {
        String tenantId = MultiTenantContext.getTenantId();
        TableRoleInCollection role = namespace.getCoord1();
        DataCollection.Version version = namespace.getCoord2();
        return Flux.fromIterable(_tableRoleTemplate.getCachedSchema(tenantId, role, version));
    }

    @Override
    public ParallelFlux<ColumnMetadata> getUnorderedSchema(Namespace2<TableRoleInCollection, DataCollection.Version> namespace) {
        return ParallelFlux.from(getSchema(namespace));
    }

    @Cacheable(cacheNames = CacheName.Constants.TableRoleMetadataCacheName, key = "T(java.lang.String).format(\"%s|%s|%s|tablerole\", #tenantId, #tableRole, #version)")
    public List<ColumnMetadata> getCachedSchema(String tenantId, TableRoleInCollection tableRole, DataCollection.Version version) {
        cdlNamespaceService.setMultiTenantContext(tenantId);
        Namespace2<String, String> servingStoreNs = cdlNamespaceService.resolveTableRole(tableRole, version);
        return proxyTemplate.getUnorderedSchema(servingStoreNs).sequential().collectList().block();
    }

}
