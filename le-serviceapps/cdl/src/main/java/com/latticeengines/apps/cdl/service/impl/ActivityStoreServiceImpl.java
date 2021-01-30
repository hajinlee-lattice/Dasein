package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.persistence.EntityNotFoundException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.entitymgr.ActivityMetricsGroupEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.AtlasStreamEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.CatalogEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataCollectionStatusEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.StreamDimensionEntityMgr;
import com.latticeengines.apps.cdl.service.ActivityStoreService;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.apps.cdl.service.DimensionMetadataService;
import com.latticeengines.common.exposed.util.DatabaseUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WithCustomerSpace;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.cdl.activity.CompositeDimensionCalculator;
import com.latticeengines.domain.exposed.cdl.activity.DeriveConfig;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("catalogService")
@Lazy
public class ActivityStoreServiceImpl implements ActivityStoreService {

    private static final Logger log = LoggerFactory.getLogger(ActivityStoreServiceImpl.class);

    private static final int MAX_FIND_RETRY = 5;

    @Inject
    private DataFeedTaskService dataFeedTaskService;

    @Inject
    private CatalogEntityMgr catalogEntityMgr;

    @Inject
    private AtlasStreamEntityMgr streamEntityMgr;

    @Inject
    private StreamDimensionEntityMgr dimensionEntityMgr;

    @Inject
    @Lazy
    private DimensionMetadataService dimensionMetadataService;

    @Inject
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Inject
    private DataCollectionStatusEntityMgr dataCollectionStatusEntityMgr;

    @Inject
    private ActivityMetricsGroupEntityMgr activityMetricsGroupEntityMgr;

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
        AtomicReference<Catalog> catalog = new AtomicReference<>();
        DatabaseUtils.retry("findCatalogByTenantAndName", MAX_FIND_RETRY, EntityNotFoundException.class,
                "EntityNotFoundException detected", null,
                input -> catalog.set(catalogEntityMgr.findByNameAndTenant(catalogName, tenant)));
        return catalog.get();
    }

    @Override
    public Catalog findCatalogByIdAndName(String customerSpace, String catalogId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(catalogId),
                String.format("CatalogId %s should not be blank", catalogId));
        Tenant tenant = MultiTenantContext.getTenant();
        AtomicReference<Catalog> catalog = new AtomicReference<>();
        DatabaseUtils.retry("findByCatalogIdAndTenant", MAX_FIND_RETRY, EntityNotFoundException.class,
                "EntityNotFoundException detected", null,
                input -> catalog.set(catalogEntityMgr.findByCatalogIdAndTenant(catalogId, tenant)));
        return catalog.get();
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
        AtomicReference<AtlasStream> atmStream = new AtomicReference<>();
        DatabaseUtils.retry("findStreamByTenantAndName", MAX_FIND_RETRY, EntityNotFoundException.class,
                "EntityNotFoundException detected", null,
                input -> atmStream.set(streamEntityMgr.findByNameAndTenant(streamName, tenant, inflateDimensions)));
        AtlasStream stream = atmStream.get();
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

    @Override
    public String saveDimensionMetadata(@NotNull String customerSpace, String signature,
            @NotNull Map<String, Map<String, DimensionMetadata>> dimensionMetadataMap) {
        Preconditions.checkNotNull(dimensionMetadataMap,
                String.format("Dimension metadata map for tenant %s should not be null", customerSpace));
        signature = newDimensionSignature(MultiTenantContext.getShortTenantId(), signature);
        log.info("Save dimension metadata for tenant {} with signature {}, # of streams = {}", customerSpace, signature,
                dimensionMetadataMap);
        dimensionMetadataService.put(signature, dimensionMetadataMap);
        return signature;
    }

    @Override
    public Map<String, DimensionMetadata> getDimensionMetadataInStream(@NotNull String customerSpace,
            @NotNull String streamName, String signature) {
        Preconditions.checkArgument(StringUtils.isNotBlank(streamName), "activity stream name should not be blank");

        String streamId = getStreamId(streamName);
        if (StringUtils.isBlank(signature)) {
            signature = getDimensionMetadataSignature(MultiTenantContext.getShortTenantId());
            if (StringUtils.isBlank(signature)) {
                return Collections.emptyMap();
            }
        }
        return dimensionMetadataService.getMetadataInStream(signature, streamId);
    }

    @Override
    public Map<String, Map<String, DimensionMetadata>> getDimensionMetadata(@NotNull String customerSpace,
            String signature) {
        return getDimensionMetadata(customerSpace, signature, true);
    }

    @Override
    public Map<String, Map<String, DimensionMetadata>> getDimensionMetadata(@NotNull String customerSpace,
            String signature, boolean withStreamName) {
        if (StringUtils.isBlank(signature)) {
            signature = getDimensionMetadataSignature(MultiTenantContext.getShortTenantId());
            if (StringUtils.isBlank(signature)) {
                return Collections.emptyMap();
            }
        }
        if (withStreamName) {
            Map<String, String> streamNameMap = getStreamNameMap(customerSpace);

            Map<String, Map<String, DimensionMetadata>> metadata = dimensionMetadataService.getMetadata(signature);

            // from streamId to streamName as key
            return metadata.entrySet().stream() //
                    .filter(entry -> streamNameMap.containsKey(entry.getKey())) //
                    .map(entry -> Pair.of(streamNameMap.get(entry.getKey()), entry.getValue())) //
                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        } else {
            return dimensionMetadataService.getMetadata(signature);
        }
    }

    @Override
    @WithCustomerSpace
    public ActivityMetricsGroup findGroupByGroupId(String customerSpace, String groupId) {
        return activityMetricsGroupEntityMgr.findByGroupId(groupId);
    }

    @Override
    @WithCustomerSpace
    public List<ActivityMetricsGroup> findByTenant(String customerSpace) {
        Tenant tenant = MultiTenantContext.getTenant();
        return activityMetricsGroupEntityMgr.findByTenant(tenant);
    }

    @Override
    public Map<String, String> allocateDimensionId(@NotNull String customerSpace,
            @NotNull Set<String> dimensionValues) {
        return dimensionMetadataService.allocateDimensionId(MultiTenantContext.getShortTenantId(), dimensionValues);
    }

    @Override
    public Map<String, String> getDimensionValues(@NotNull String customerSpace, @NotNull Set<String> dimensionIds) {
        return dimensionMetadataService.getDimensionValues(MultiTenantContext.getShortTenantId(), dimensionIds);
    }

    @Override
    public Map<String, String> getDimensionIds(String tenantId, Set<String> dimensionValues) {
        return dimensionMetadataService.getDimensionIds(MultiTenantContext.getShortTenantId(), dimensionValues);
    }

    @Override
    public List<AtlasStream> getStreams(String customerSpace) {
        List<AtlasStream> streams = streamEntityMgr.findByTenant(MultiTenantContext.getTenant());
        for (AtlasStream stream : streams) {
            stream.setTenant(null);
            stream.setDimensions(null);
        }
        return streams;
    }

    @Override
    @WithCustomerSpace
    public List<AtlasStream> getStreamsByStreamType(String customerSpace, AtlasStream.StreamType streamType) {
        List<AtlasStream> streams = streamEntityMgr.findByTenantAndStreamType(MultiTenantContext.getTenant(),
                streamType);
        return CollectionUtils.isEmpty(streams) ? Collections.emptyList() : streams;
    }

    // streamId -> streamName
    @Override
    @WithCustomerSpace
    public Map<String, String> getStreamNameMap(@NotNull String customerSpace) {
        List<AtlasStream> streams = streamEntityMgr.findByTenant(MultiTenantContext.getTenant());
        if (CollectionUtils.isEmpty(streams)) {
            return Collections.emptyMap();
        }
        return streams.stream()
                .collect(Collectors.toMap(AtlasStream::getStreamId, AtlasStream::getName, (v1, v2) -> v1));
    }

    @Override
    @WithCustomerSpace
    public Map<AtlasStream.StreamType, List<String>> getStreamTypeToStreamNamesMap(String customerSpace) {
        Tenant tenant = MultiTenantContext.getTenant();
        Map<AtlasStream.StreamType, List<String>> map = new HashMap<>();
        Arrays.stream(AtlasStream.StreamType.values()).forEach(type -> {
            List<AtlasStream> streams = streamEntityMgr.findByTenantAndStreamType(tenant, type);
            if (CollectionUtils.isNotEmpty(streams)) {
                map.put(type, streams.stream().map(AtlasStream::getName).collect(Collectors.toList()));
            } else {
                map.put(type, Collections.emptyList());
            }
        });
        return map;
    }

    @Override
    @WithCustomerSpace
    public boolean addDeriveDimensionConfig(String customerSpace, String streamName, DeriveConfig config) {
        Tenant tenant = MultiTenantContext.getTenant();
        AtlasStream stream = streamEntityMgr.findByNameAndTenant(streamName, tenant);
        if (stream == null) {
            throw new IllegalArgumentException(String.format("Unable to find stream with name %s for tenant %s", streamName, tenant.getName()));
        }
        StreamDimension dimension = new StreamDimension();
        log.info("Creating derive config: {}", config);
        dimension.setDeriveConfig(config);
        dimension.setName(InterfaceName.DerivedId.name());
        dimension.setDisplayName(InterfaceName.DerivedId.name());
        dimension.setTenant(tenant);
        dimension.setStream(stream);
        dimension.addUsages(StreamDimension.Usage.Pivot);
        dimension.setCalculator(getDeriveCalculator(config));
        dimension.setGenerator(getDeriveGenerator());
        dimensionEntityMgr.create(dimension);
        return true;
    }

    private DimensionGenerator getDeriveGenerator() {
        DimensionGenerator generator = new DimensionGenerator();
        generator.setAttribute(InterfaceName.DerivedName.name());
        generator.setFromCatalog(false);
        generator.setOption(DimensionGenerator.DimensionGeneratorOption.DERIVE);
        return generator;
    }

    private DimensionCalculator getDeriveCalculator(DeriveConfig config) {
        CompositeDimensionCalculator calculator = new CompositeDimensionCalculator();
        calculator.deriveConfig = config;
        return calculator;
    }

    private String getStreamId(@NotNull String streamName) {
        String tenantId = MultiTenantContext.getShortTenantId();
        AtlasStream stream = streamEntityMgr.findByNameAndTenant(streamName, MultiTenantContext.getTenant());
        Preconditions.checkArgument(stream != null && streamName.equals(stream.getName()),
                String.format("No activity stream found with name %s in tenant %s", streamName, tenantId));
        return stream.getStreamId();
    }

    private String getDimensionMetadataSignature(@NotNull String customerSpace) {
        DataCollection.Version activeVersion = dataCollectionEntityMgr.findActiveVersion();
        if (activeVersion == null) {
            log.info("No current active version found for tenant {}", customerSpace);
            return null;
        }
        DataCollectionStatus status = dataCollectionStatusEntityMgr
                .findByTenantAndVersion(MultiTenantContext.getTenant(), activeVersion);
        if (status == null) {
            log.info("No datacollection status for active version found in tenant {}", customerSpace);
            return null;
        }
        String signature = status.getDimensionMetadataSignature();
        log.info("Found dimension metadata signature in tenant {}, activeVersion = {}, signature = {}", customerSpace,
                activeVersion, signature);
        return signature;
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

    private String newDimensionSignature(@NotNull String tenantId, String signature) {
        if (StringUtils.isEmpty(signature)) {
            signature = UuidUtils.shortenUuid(UUID.randomUUID());
        }
        return String.format("%s_%s", tenantId, signature);
    }
}
