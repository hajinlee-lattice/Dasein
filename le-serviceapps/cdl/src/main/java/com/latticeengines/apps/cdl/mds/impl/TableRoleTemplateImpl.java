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
import com.latticeengines.domain.exposed.metadata.datastore.DataTemplateName;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnitStore;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.metadata.service.DataTemplateService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Component("tableRoleTemplate")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class TableRoleTemplateImpl implements TableRoleTemplate {

    private final CDLNamespaceService cdlNamespaceService;
    private final TableRoleTemplate _tableRoleTemplate;
    private final DataUnitStore<Namespace2<String, String>> proxyTemplate;

    private static final Scheduler scheduler = Schedulers.newParallel("tablerole-template");

    @Inject
    public TableRoleTemplateImpl(DataTemplateService dataTemplateService, CDLNamespaceService cdlNamespaceService,
            TableRoleTemplate tableRoleTemplate) {
        this.cdlNamespaceService = cdlNamespaceService;
        this.proxyTemplate = new DataUnitStore<Namespace2<String, String>>() {
            @Override
            public List<DataUnit> getData(Namespace2<String, String> namespace2) {
                return dataTemplateService.getData(DataTemplateName.Table, namespace2.getCoord1(),
                        namespace2.getCoord2());
            }

            @Override
            public Flux<ColumnMetadata> getSchema(Namespace2<String, String> namespace2) {
                return dataTemplateService.getSchema(DataTemplateName.Table, namespace2.getCoord1(),
                        namespace2.getCoord2());
            }

            @Override
            public ParallelFlux<ColumnMetadata> getUnorderedSchema(Namespace2<String, String> namespace2) {
                return dataTemplateService.getUnorderedSchema(DataTemplateName.Table, namespace2.getCoord1(),
                        namespace2.getCoord2());
            }
        };
        this._tableRoleTemplate = tableRoleTemplate;
    }

    @Override
    public List<DataUnit> getData(Namespace2<TableRoleInCollection, DataCollection.Version> namespace) {
        Namespace2<String, String> servingStoreNs = cdlNamespaceService.resolveTableRole(namespace.getCoord1(),
                namespace.getCoord2());
        return proxyTemplate.getData(servingStoreNs);
    }

    @Override
    public Flux<ColumnMetadata> getSchema(Namespace2<TableRoleInCollection, DataCollection.Version> namespace) {
        String tenantId = MultiTenantContext.getShortTenantId();
        TableRoleInCollection role = namespace.getCoord1();
        DataCollection.Version version = namespace.getCoord2();
        if (cdlNamespaceService.hasTableRole(role, version)) {
            return Flux.fromIterable(_tableRoleTemplate.getCachedSchema(tenantId, role, version));
        } else {
            return Flux.empty();
        }
    }

    @Override
    public ParallelFlux<ColumnMetadata> getUnorderedSchema(
            Namespace2<TableRoleInCollection, DataCollection.Version> namespace) {
        return getSchema(namespace).parallel().runOn(scheduler);
    }

    @Cacheable(cacheNames = CacheName.Constants.TableRoleMetadataCacheName, key = "T(java.lang.String).format(\"%s|%s|%s|tablerole\", #tenantId, #tableRole, #version)")
    public List<ColumnMetadata> getCachedSchema(String tenantId, TableRoleInCollection tableRole,
            DataCollection.Version version) {
        cdlNamespaceService.setMultiTenantContext(tenantId);
        Namespace2<String, String> servingStoreNs = cdlNamespaceService.resolveTableRole(tableRole, version);
        return proxyTemplate.getUnorderedSchema(servingStoreNs).sequential().collectList().block();
    }

}
