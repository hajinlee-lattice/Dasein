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
    public Catalog create(@NotNull String customerSpace, @NotNull String catalogName, String taskUniqueId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(catalogName), "catalog name should not be blank");
        Tenant tenant = MultiTenantContext.getTenant();
        Catalog catalog = new Catalog();
        catalog.setName(catalogName);
        catalog.setTenant(tenant);
        catalog.setDataFeedTask(getDataFeedTask(tenant, taskUniqueId));
        catalogEntityMgr.create(catalog);
        return catalog;
    }

    @Override
    public Catalog findByTenantAndName(@NotNull String customerSpace, @NotNull String catalogName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(catalogName), String.format("CatalogName %s should not be blank", catalogName));
        Tenant tenant = MultiTenantContext.getTenant();
        return catalogEntityMgr.findByNameAndTenant(catalogName, tenant);
    }

    private DataFeedTask getDataFeedTask(@NotNull Tenant tenant, String taskId) {
        if (StringUtils.isBlank(taskId)) {
            return null;
        }

        DataFeedTask dataFeedTask = dataFeedTaskService.getDataFeedTask(tenant.getId(), taskId);
        if (dataFeedTask == null || dataFeedTask.getDataFeed() == null) {
            throw new IllegalArgumentException(
                    String.format("Provided DataFeedTask uniqueId %s does not match any existing feed task", taskId));
        }
        Tenant taskTenant = dataFeedTask.getDataFeed().getTenant();
        if (taskTenant == null || taskTenant.getId() == null || !taskTenant.getId().equals(tenant.getId())) {
            throw new IllegalArgumentException(String.format("Tenant for input DataFeedTask %s is not the same as input tenant %s", taskTenant, tenant));
        }
        return dataFeedTask;
    }
}
