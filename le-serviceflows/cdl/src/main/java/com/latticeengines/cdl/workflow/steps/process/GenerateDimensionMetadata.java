package com.latticeengines.cdl.workflow.steps.process;

import static com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator.DimensionGeneratorOption.BOOLEAN;
import static com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator.DimensionGeneratorOption.DERIVE;
import static com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator.DimensionGeneratorOption.ENUM;
import static com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator.DimensionGeneratorOption.HASH;
import static com.latticeengines.domain.exposed.cdl.activity.StreamDimension.Usage.Pivot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculatorRegexMode;
import com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ActivityStreamSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ProcessDimensionConfig;
import com.latticeengines.domain.exposed.util.TypeConversionUtil;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.ProcessDimensionJob;

@Component(GenerateDimensionMetadata.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class GenerateDimensionMetadata
        extends RunSparkJob<ActivityStreamSparkStepConfiguration, ProcessDimensionConfig> {
    private static final Logger log = LoggerFactory.getLogger(GenerateDimensionMetadata.class);
    // limit of total possible dimension value combination per stream
    private static final int DIMENSION_CARDINALITY_LIMIT = 2000;

    static final String BEAN_NAME = "generateDimensionMetadata";

    @Inject
    private ActivityStoreProxy activityStoreProxy;

    // [streamId, dimName] <-> UUID
    private BiMap<Pair<String, String>, String> dimIdMap = HashBiMap.create();
    private List<DataUnit> units = new ArrayList<>();
    // tableName -> idx in units
    private Map<String, Integer> unitIdxMap = new HashMap<>();
    private Map<String, String> streamErrorMsgs = new HashMap<>();
    // warning msgs that should be surfaced to user
    private Set<String> streamWarningMsgs = new HashSet<>();
    // streamId -> set of dimName that has no data
    private Map<String, Set<String>> emptyMetadataDimensions = new HashMap<>();

    @Override
    protected ProcessDimensionConfig configureJob(ActivityStreamSparkStepConfiguration stepConfiguration) {
        // TODO -skip this one
        Map<String, AtlasStream> streams = stepConfiguration.getActivityStreamMap();
        Map<String, Table> catalogTableNames = getTablesFromMapCtxKey(customerSpace.toString(), CATALOG_TABLE_NAME);
        Map<String, Table> rawStreamTableNames = getTablesFromMapCtxKey(customerSpace.toString(),
                RAW_ACTIVITY_STREAM_TABLE_NAME);

        ProcessDimensionConfig config = new ProcessDimensionConfig();
        config.collectMetadata = true;
        config.dimensions = buildDimensions(streams, catalogTableNames, rawStreamTableNames);
        config.setInput(units);

        if (MapUtils.isEmpty(config.dimensions)) {
            log.info("No pivot dimensions need processing, skip generating dimension metadata");

            // create an artificial metadata map
            Map<String, Map<String, DimensionMetadata>> streamDimensionMetadatas = new HashMap<>();
            addEmptyMetadata(streamDimensionMetadatas);
            addBooleanPivotMetadata(streamDimensionMetadatas, configuration.getActivityStreamMap());
            saveDimensionMetadataMap(streamDimensionMetadatas);
            return null;
        }

        log.info("ProcessDimensionConfig = {}", JsonUtils.serialize(config));
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        Map<String, Map<String, DimensionMetadata>> dimensionMetadataMap = getDimensionMetadataMap(result);
        dimensionMetadataMap.forEach((streamId, metadataMap) -> {
            // calculate product of all dimension cardinality in stream
            Long streamCardinality = metadataMap.entrySet() //
                    .stream() //
                    .filter(Objects::nonNull) //
                    .map(Map.Entry::getValue) //
                    .filter(Objects::nonNull) //
                    .map(DimensionMetadata::getDimensionValues) //
                    .map(CollectionUtils::size) //
                    .map(Integer::longValue) //
                    .reduce(1L, (v1, v2) -> v1 * v2);
            if (streamCardinality != null && streamCardinality > DIMENSION_CARDINALITY_LIMIT) {
                log.warn("Stream {} has total dimension cardinality {}, exceeding limit {}, will not process further",
                        streamId, streamCardinality, DIMENSION_CARDINALITY_LIMIT);
                streamErrorMsgs.put(streamId, String.format(
                        "Stream %s has too many distinct values for its dimensions, skip processing", streamId));
            }
        });
        removeSkippedStreamMetadata(dimensionMetadataMap);
        allocateDimensionIdsAndOverrideMap(dimensionMetadataMap);
        mergeMetadata(dimensionMetadataMap);
        saveDimensionMetadataMap(dimensionMetadataMap);
    }

    private void removeSkippedStreamMetadata(Map<String, Map<String, DimensionMetadata>> dimensionMetadataMap) {
        streamErrorMsgs.keySet().forEach(dimensionMetadataMap::remove);
    }

    private void saveDimensionMetadataMap(@NotNull Map<String, Map<String, DimensionMetadata>> dimensionMetadataMap) {
        log.info("Final dimension metadata map = {}", JsonUtils.serialize(dimensionMetadataMap));
        putObjectInContext(STREAM_DIMENSION_METADATA_MAP, dimensionMetadataMap);
        recordStreamSkippedForAgg();
        // publish metadata for serving in app
        String signature = activityStoreProxy.saveDimensionMetadata(configuration.getCustomer(), dimensionMetadataMap);
        saveDimensionMetadataSignature(signature);
    }

    // generate short ID for each unique dimension value and update corresponding
    // map
    private void allocateDimensionIdsAndOverrideMap(Map<String, Map<String, DimensionMetadata>> dimensionMetadataMap) {

        Set<String> values = dimensionMetadataMap //
                .values() //
                .stream() //
                .flatMap(dims -> dims.entrySet().stream().flatMap(dimMetadata -> {
                    String dimName = dimMetadata.getKey();
                    return dimMetadata.getValue().getDimensionValues().stream().map(attrs -> attrs.get(dimName))
                            .map(TypeConversionUtil::toString);
                })) //
                .filter(StringUtils::isNotBlank) //
                .collect(Collectors.toSet());
        if (CollectionUtils.isEmpty(values)) {
            return;
        }

        // allocate short ID
        Map<String, String> valueIdMap = activityStoreProxy.allocateDimensionIds(customerSpace.toString(), values);
        putObjectInContext(STREAM_DIMENSION_VALUE_ID_MAP, valueIdMap);

        log.info("DimensionValueIdMap = {}", valueIdMap);

        // update metadata - replace dimension name with short ID
        dimensionMetadataMap.forEach((streamId, dims) -> {
            dims.forEach((dimName, metadata) -> {
                if (CollectionUtils.isEmpty(metadata.getDimensionValues())) {
                    return;
                }

                metadata.getDimensionValues().forEach(attrs -> {
                    String value = TypeConversionUtil.toString(attrs.get(dimName));
                    if (valueIdMap.containsKey(value)) {
                        // override with short ID
                        attrs.put(dimName, valueIdMap.get(value));
                    }
                });
            });
        });
    }

    private void mergeMetadata(Map<String, Map<String, DimensionMetadata>> dimensionMetadataMap) {
        dimensionMetadataMap.forEach((streamId, dimensions) -> {
            AtlasStream stream = configuration.getActivityStreamMap().get(streamId);
            stream.getDimensions().forEach(dimension -> {
                if (BooleanUtils.isFalse(dimension.getShouldReplace())) {
                    Map<String, DimensionMetadata> activeDimensionMetadata = getStreamActiveDimensionMetadata(stream);
                    String dimName = dimension.getName();
                    log.info("Merging dimension dimension {} in stream {}. Active={}, New={}",
                            dimension.getName(),
                            stream.getStreamId(),
                            JsonUtils.serialize(activeDimensionMetadata),
                            dimName);
                    DimensionMetadata merged = mergeDimensionMetadata(dimName, dimensionMetadataMap.get(streamId).get(dimName), activeDimensionMetadata.get(dimName));
                    dimensionMetadataMap.get(streamId).put(dimName, merged);
                }
            });
        });
    }

    // TODO - maybe put this in an util and write unit test?
    private DimensionMetadata mergeDimensionMetadata(String dimName, DimensionMetadata newMetadata, DimensionMetadata activeMetadata) {
        if (activeMetadata == null) {
            return newMetadata;
        }
        // active <dimName value> -> <dim object map>
        Map<String, Map<String, Object>> activeDimMap = activeMetadata.getDimensionValues().stream()
                .map(dimObj -> Pair.of(dimObj.get(dimName).toString(), dimObj))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        DimensionMetadata merged = new DimensionMetadata();
        merged.setDimensionValues(new ArrayList<>(activeMetadata.getDimensionValues()));
        merged.getDimensionValues().addAll(newMetadata.getDimensionValues().stream()
                .filter(dimObj -> !activeDimMap.containsKey(dimObj.get(dimName).toString())) // fetch dimObject with names not in active dimension metadata
                .collect(Collectors.toList()));
        merged.setCardinality(merged.getDimensionValues().size());
        return merged;
    }

    private Map<String, DimensionMetadata> getStreamActiveDimensionMetadata(AtlasStream stream) {
        Map<String, DimensionMetadata> dimensionMetadataMap = new HashMap<>();
        try {
            dimensionMetadataMap = activityStoreProxy.getDimensionMetadataInStream(customerSpace.toString(), stream.getName(), null);
            log.info("Stream {} configured to merge dimension metadata. Found active metadata {}", stream.getStreamId(), JsonUtils.serialize(dimensionMetadataMap));
        } catch (Exception e) {
            log.info("Stream {} configured to merge dimension metadata. Failed to retrive active metadata. {}", stream.getStreamId(), e);
        }
        return dimensionMetadataMap;
    }

    private void saveDimensionMetadataSignature(@NotNull String signature) {
        DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        status.setDimensionMetadataSignature(signature);
        putObjectInContext(CDL_COLLECTION_STATUS, status);
        log.info("Save dimension metadata signature {}", signature);
    }

    private void recordStreamSkippedForAgg() {
        Set<String> streamsToSkipAgg = streamErrorMsgs.keySet();
        if (CollectionUtils.isNotEmpty(streamsToSkipAgg)) {
            putObjectInContext(ACTIVITY_STREAMS_SKIP_AGG, streamsToSkipAgg);
            log.warn("Some streams are skipped for aggregation. errorMsgs = {}", streamErrorMsgs);
        }

        streamWarningMsgs.forEach(msg -> addToListInContext(PROCESS_ANALYTICS_WARNING_KEY, msg, String.class));
    }

    private Map<String, ProcessDimensionConfig.Dimension> buildDimensions(Map<String, AtlasStream> streams,
                                                                          Map<String, Table> catalogTables, Map<String, Table> rawStreamTables) {
        if (MapUtils.isEmpty(streams)) {
            return Collections.emptyMap();
        }

        return streams.values().stream().flatMap(stream -> {
            String streamId = stream.getStreamId();
            List<StreamDimension> dimensions = stream.getDimensions();
            if (CollectionUtils.isEmpty(dimensions)) {
                streamErrorMsgs.put(streamId, "No dimension configured for this stream");
                return Stream.empty();
            }
            // make sure stream & catalog has data TODO maybe add warning
            if (!rawStreamTables.containsKey(streamId)) {
                // no new import nor batch store
                log.info("No raw stream data for stream {}, skip generating metadata", streamId);
                streamErrorMsgs.put(streamId, "No data for this stream");
                logNoDataStream(stream);
                return Stream.empty();
            }

            return getDimensionConfigs(streamId, dimensions, catalogTables, rawStreamTables);
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    /*-
     * Generate spark job config for all pivot dimensions
     * TODO need to handle other usages?
     */
    private Stream<Pair<String, ProcessDimensionConfig.Dimension>> getDimensionConfigs(String streamId,
                                                                                       List<StreamDimension> dimensions, Map<String, Table> catalogTables, Map<String, Table> rawStreamTables) {
        return dimensions.stream().filter(this::isPivotDimension).map(dimension -> {
            if (dimension.getGenerator().getOption() == BOOLEAN) {
                return null;
            }
            String dimName = dimension.getName();
            String dimId = UUID.randomUUID().toString();
            DimensionGenerator generator = dimension.getGenerator();
            DimensionCalculator calculator = dimension.getCalculator();
            ProcessDimensionConfig.Dimension dim = generator.getOption() == DERIVE ?
                    new ProcessDimensionConfig.DerivedDimension() : new ProcessDimensionConfig.Dimension();
            // save id mapping
            dimIdMap.put(Pair.of(streamId, dimName), dimId);
            // set input
            if (generator.isFromCatalog()) {
                Preconditions.checkNotNull(dimension.getCatalog(),
                        String.format("Dimension %s in stream %s must have an associated catalog", dimName, streamId));
                String catalogId = dimension.getCatalog().getCatalogId();
                if (!catalogTables.containsKey(catalogId)) {
                    log.info(
                            "Catalog {} in dimName {} and stream {} does not have any data, generate empty metadata for it",
                            catalogId, dimName, streamId);
                    logNoDataCatalog(dimension.getCatalog());
                    emptyMetadataDimensions.putIfAbsent(streamId, new HashSet<>());
                    emptyMetadataDimensions.get(streamId).add(dimName);
                    return null;
                }
                dim.inputIdx = addTableAndGetDataUnitIdx(catalogId, catalogTables, "Catalog_");
            } else {
                dim.inputIdx = addTableAndGetDataUnitIdx(streamId, rawStreamTables, "Stream_");
            }

            dim.attrs = Sets.newHashSet(dimName);
            dim.dedupAttrs = Collections.singleton(dimName);
            dim.valueLimit = DIMENSION_CARDINALITY_LIMIT;
            if (generator.getOption() == HASH) {
                dim.attrs.add(generator.getAttribute()); // save the original value for reference
                dim.hashAttrs = Collections.singletonMap(generator.getAttribute(), dimName);
            } else if (generator.getOption() == ENUM) {
                dim.renameAttrs = Collections.singletonMap(generator.getAttribute(), dimName);
            } else if (generator.getOption() == DERIVE) {
                dim.attrs = Sets.newHashSet(InterfaceName.DerivedName.name(), InterfaceName.DerivedId.name(), InterfaceName.DerivedPattern.name());
                dim.dedupAttrs = Collections.singleton(InterfaceName.DerivedId.name());
                assert dim instanceof ProcessDimensionConfig.DerivedDimension;
                ((ProcessDimensionConfig.DerivedDimension) dim).deriveConfig = dimension.getDeriveConfig();
            }
            if (calculator instanceof DimensionCalculatorRegexMode) {
                // assume calculator pattern can only come from the same source as generator for
                // now
                DimensionCalculatorRegexMode regCalculator = (DimensionCalculatorRegexMode) calculator;
                dim.attrs.add(regCalculator.getPatternAttribute());
            }

            return Pair.of(dimId, dim);
        }).filter(Objects::nonNull);
    }

    private void logNoDataStream(AtlasStream stream) {
        if (stream == null) {
            return;
        }

        String displayName = getStreamTypeDisplayName(stream.getStreamType());
        if (StringUtils.isBlank(displayName)) {
            return;
        }

        streamWarningMsgs.add(String.format("%s template has been set up but no data ingested yet", displayName));
    }

    private String getStreamTypeDisplayName(AtlasStream.StreamType type) {
        if (type == AtlasStream.StreamType.WebVisit) {
            return "Website Visit";
        } else if (type == AtlasStream.StreamType.Opportunity) {
            return "Opportunity";
        } else if (type == AtlasStream.StreamType.MarketingActivity) {
            return "Marketing Activity";
        } else if (type == AtlasStream.StreamType.DnbIntentData) {
            return "D&B Intent";
        }
        return null;
    }

    private void logNoDataCatalog(Catalog catalog) {
        if (catalog == null || StringUtils.isBlank(catalog.getName())) {
            return;
        }

        String[] displayNames = getCatalogStreamDisplayNames(catalog.getName());
        if (displayNames == null || displayNames.length < 2) {
            return;
        }

        streamWarningMsgs.add(String.format("%s is not configured yet. %s attributes will not be generated",
                displayNames[0], displayNames[1]));
    }

    private String[] getCatalogStreamDisplayNames(@NotNull String catalogName) {
        if (catalogName.contains(EntityType.WebVisitPathPattern.name())) {
            return new String[] { "Website Path", "Website Visit" };
        } else if (catalogName.contains(EntityType.Opportunity.name())) {
            return new String[] { "Opportunity Stage", "Opportunity" };
        } else if (catalogName.contains(EntityType.MarketingActivity.name())) {
            return new String[] { "Marketing Activity Type", "Marketing Activity" };
        }
        return null;
    }

    // streamId -> {dimId -> dimMetadata}
    private Map<String, Map<String, DimensionMetadata>> getDimensionMetadataMap(SparkJobResult result) {
        Map<?, ?> rawMetadataMap = JsonUtils.deserialize(result.getOutput(), Map.class);
        // dimId -> metadata
        Map<String, DimensionMetadata> metadataMap = JsonUtils.convertMap(rawMetadataMap, String.class,
                DimensionMetadata.class);
        if (MapUtils.isEmpty(metadataMap)) {
            return Collections.emptyMap();
        }
        // streamId -> { dimensionName -> metadata }
        Map<String, Map<String, DimensionMetadata>> streamDimensionMetadatas = new HashMap<>();
        metadataMap.forEach((dimId, metadata) -> {
            Pair<String, String> streamDim = dimIdMap.inverse().get(dimId);
            String streamId = streamDim.getLeft();
            String dimName = streamDim.getRight();
            streamDimensionMetadatas.putIfAbsent(streamId, new HashMap<>());
            streamDimensionMetadatas.get(streamId).put(dimName, metadata);
        });

        // add artificial metadata
        addEmptyMetadata(streamDimensionMetadatas);
        addBooleanPivotMetadata(streamDimensionMetadatas, configuration.getActivityStreamMap());
        return streamDimensionMetadatas;
    }

    // add empty metadata for dimension without data
    private void addEmptyMetadata(@NotNull Map<String, Map<String, DimensionMetadata>> metadataMap) {
        emptyMetadataDimensions.forEach((streamId, dimNames) -> {
            DimensionMetadata metadata = new DimensionMetadata();
            metadata.setDimensionValues(Collections.emptyList());
            metadataMap.putIfAbsent(streamId, new HashMap<>());
            dimNames.forEach(dimName -> metadataMap.get(streamId).put(dimName, metadata));
        });
    }

    // add metadata for Usage=Pivot & GeneratorOption=BOOLEAN
    private void addBooleanPivotMetadata(@NotNull Map<String, Map<String, DimensionMetadata>> metadataMap,
                                         @NotNull Map<String, AtlasStream> streamMap) {
        streamMap.values().stream() //
                .filter(Objects::nonNull) //
                .filter(stream -> CollectionUtils.isNotEmpty(stream.getDimensions())) //
                .forEach(stream -> {
                    String streamId = stream.getStreamId();
                    metadataMap.putIfAbsent(streamId, new HashMap<>());
                    stream.getDimensions().stream().filter(this::isPivotBooleanDimension).forEach(dim -> {
                        DimensionMetadata metadata = new DimensionMetadata();
                        metadata.setCardinality(2L);
                        metadata.setDimensionValues(getBooleanDimensionValues(dim.getName()));
                        metadataMap.get(streamId).put(dim.getName(), metadata);
                    });
                });
    }

    private List<Map<String, Object>> getBooleanDimensionValues(String dimensionAttr) {
        List<Map<String, Object>> values = new ArrayList<>();
        values.add(Collections.singletonMap(dimensionAttr, true));
        values.add(Collections.singletonMap(dimensionAttr, false));
        return values;
    }

    private boolean isPivotBooleanDimension(StreamDimension dimension) {
        return isPivotDimension(dimension) && dimension.getGenerator() != null
                && dimension.getGenerator().getOption() == BOOLEAN;
    }

    private boolean isPivotDimension(StreamDimension dimension) {
        return dimension != null && CollectionUtils.isNotEmpty(dimension.getUsages())
                && dimension.getUsages().contains(Pivot);
    }

    private int addTableAndGetDataUnitIdx(String id, Map<String, Table> tables, String unitAliasPrefix) {
        if (!unitIdxMap.containsKey(id)) {
            unitIdxMap.put(id, units.size());
            units.add(tables.get(id).partitionedToHdfsDataUnit(unitAliasPrefix + id, Collections.singletonList(InterfaceName.StreamDateId.name())));
        }
        return unitIdxMap.get(id);
    }

    @Override
    protected Class<? extends AbstractSparkJob<ProcessDimensionConfig>> getJobClz() {
        return ProcessDimensionJob.class;
    }
}
