package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.entitymgr.CatalogEntityMgr;
import com.latticeengines.apps.cdl.service.CatalogService;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("catalogService")
public class CatalogServiceImpl implements CatalogService {

    @Inject
    private DataFeedTaskService dataFeedTaskService;

    @Inject
    private CatalogEntityMgr catalogEntityMgr;

    @Override
    public Catalog create(@NotNull String customerSpace, @NotNull Catalog catalog) {
        checkCatalog(catalog);
        Tenant tenant = MultiTenantContext.getTenant();
        catalog.setTenant(tenant);
        checkDataFeedTask(tenant, catalog.getDataFeedTask());
        catalogEntityMgr.create(catalog);
        return catalog;
    }

    @Override
    public Catalog findByTenantAndName(@NotNull String customerSpace, @NotNull String catalogName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(catalogName), String.format("CatalogName %s should not be blank", catalogName));
        Tenant tenant = MultiTenantContext.getTenant();
        return catalogEntityMgr.findByNameAndTenant(catalogName, tenant);
    }

    private void checkDataFeedTask(@NotNull Tenant tenant, DataFeedTask task) {
        if (task == null) {
            return;
        }

        String taskId = task.getUniqueId();
        task = dataFeedTaskService.getDataFeedTask(tenant.getId(), taskId);
        if (task == null || task.getDataFeed() == null) {
            throw new IllegalArgumentException(String.format("Input DataFeedTask(uniqueId=%s) not exist", taskId));
        }
        Tenant taskTenant = task.getDataFeed().getTenant();
        if (taskTenant == null || taskTenant.getId() == null || !taskTenant.getId().equals(tenant.getId())) {
            throw new IllegalArgumentException(String.format("Tenant for input DataFeedTask %s is not the same as input tenant %s", taskTenant, tenant));
        }
    }

    private void checkCatalog(@NotNull Catalog catalog) {
        Preconditions.checkNotNull(catalog, "Catalog should not be null");
        Preconditions.checkArgument(StringUtils.isNotBlank(catalog.getName()), "Catalog should have non blank name");
    }
}
