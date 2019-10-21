package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.entitymgr.AtlasStreamEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.CatalogEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.StreamDimensionEntityMgr;
import com.latticeengines.apps.cdl.service.ActivityStoreService;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("catalogService")
public class ActivityStoreServiceImpl implements ActivityStoreService {

    @Inject
    private DataFeedTaskService dataFeedTaskService;

    @Inject
    private CatalogEntityMgr catalogEntityMgr;

    @Inject
    private AtlasStreamEntityMgr streamEntityMgr;

    @Inject
    private StreamDimensionEntityMgr dimensionEntityMgr;

    @Override
    public Catalog createCatalog(@NotNull String customerSpace, @NotNull String catalogName, String taskUniqueId,
            String primaryKeyColumn) {
        // TODO retry on catalogId conflict
        Preconditions.checkArgument(StringUtils.isNotBlank(catalogName), "catalog name should not be blank");
        Tenant tenant = MultiTenantContext.getTenant();
        Catalog catalog = new Catalog();
        catalog.setName(catalogName);
        catalog.setCatalogId(Catalog.generateId());
        catalog.setTenant(tenant);
        catalog.setPrimaryKeyColumn(primaryKeyColumn);
        catalog.setDataFeedTask(getDataFeedTask(tenant, taskUniqueId));
        catalogEntityMgr.create(catalog);
        return catalog;
    }

    @Override
    public Catalog findCatalogByTenantAndName(@NotNull String customerSpace, @NotNull String catalogName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(catalogName),
                String.format("CatalogName %s should not be blank", catalogName));
        Tenant tenant = MultiTenantContext.getTenant();
        return catalogEntityMgr.findByNameAndTenant(catalogName, tenant);
    }

    @Override
    public Catalog findCatalogByIdAndName(String customerSpace, String catalogId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(catalogId),
                String.format("CatalogId %s should not be blank", catalogId));
        Tenant tenant = MultiTenantContext.getTenant();
        return catalogEntityMgr.findByCatalogIdAndTenant(catalogId, tenant);
    }

    @Override
    public AtlasStream createStream(@NotNull String customerSpace, @NotNull AtlasStream stream) {
        // TODO retry on streamId conflict
        Preconditions.checkNotNull(stream, "stream to be created should not be null");
        Preconditions.checkNotNull(stream.getDataFeedTaskUniqueId(), "stream should contains data feed task unique ID");
        Preconditions.checkArgument(StringUtils.isBlank(stream.getStreamId()),
                "stream should not contain streamId field which is auto generated");
        Tenant tenant = MultiTenantContext.getTenant();

        stream.setDataFeedTask(getDataFeedTask(tenant, stream.getDataFeedTaskUniqueId()));
        stream.setStreamId(AtlasStream.generateId());
        streamEntityMgr.create(stream);

        // create attached dimensions
        if (CollectionUtils.isNotEmpty(stream.getDimensions())) {
            stream.getDimensions().forEach(dimension -> {
                dimension.setStream(stream);
                dimensionEntityMgr.create(dimension);
            });
        }
        return stream;
    }

    @Override
    public AtlasStream findStreamByTenantAndName(@NotNull String customerSpace, @NotNull String streamName,
            boolean inflateDimensions) {
        Preconditions.checkArgument(StringUtils.isNotBlank(streamName),
                String.format("StreamName %s should not be blank", streamName));
        Tenant tenant = MultiTenantContext.getTenant();
        AtlasStream stream = streamEntityMgr.findByNameAndTenant(streamName, tenant, inflateDimensions);
        if (!inflateDimensions && stream != null) {
            // set empty list to prevent lazy loading uninitialized proxy
            stream.setDimensions(Collections.emptyList());
        }
        return stream;
    }

    @Override
    public StreamDimension updateStreamDimension(@NotNull String customerSpace, @NotNull String streamName,
            @NotNull StreamDimension dimension) {
        Preconditions.checkNotNull(dimension, "dimension to be updated should not be null");
        Preconditions.checkNotNull(dimension.getName());
        Tenant tenant = MultiTenantContext.getTenant();

        // check given stream/dimension exist
        AtlasStream stream = streamEntityMgr.findByNameAndTenant(streamName, tenant, true);
        Preconditions.checkNotNull(stream, "target stream does not exist");
        Preconditions.checkArgument(stream.getDimensions() != null && !stream.getDimensions().isEmpty(),
                "target stream does not have any dimension");
        boolean exists = stream.getDimensions().stream().anyMatch(dim -> dimension.getName().equals(dim.getName()));
        Preconditions.checkArgument(exists,
                String.format("Dimension %s does not exist under stream %s", dimension.getName(), streamName));

        dimension.setStream(stream);
        dimensionEntityMgr.update(dimension);

        // return the updated version
        return dimensionEntityMgr.findByNameAndTenantAndStream(dimension.getName(), tenant, stream);
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
            throw new IllegalArgumentException(String
                    .format("Tenant for input DataFeedTask %s is not the same as input tenant %s", taskTenant, tenant));
        }
        return dataFeedTask;
    }
}
