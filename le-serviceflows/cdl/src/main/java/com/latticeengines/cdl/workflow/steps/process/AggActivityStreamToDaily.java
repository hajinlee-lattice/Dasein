package com.latticeengines.cdl.workflow.steps.process;

import static com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator.DimensionGeneratorOption.DERIVE;
import static com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator.DimensionGeneratorOption.HASH;
import static com.latticeengines.domain.exposed.cdl.activity.StreamDimension.Usage.Dedup;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.StreamDateId;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AggregatedActivityStream;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AggregatedActivityStreamDelta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroupUtils;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicy;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicyTimeUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ActivityStreamSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.AggDailyActivityConfig;
import com.latticeengines.domain.exposed.spark.cdl.SparkIOMetadataWrapper;
import com.latticeengines.domain.exposed.util.RetentionPolicyUtil;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.AggDailyActivityJob;

@Component(AggActivityStreamToDaily.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class AggActivityStreamToDaily
        extends RunSparkJob<ActivityStreamSparkStepConfiguration, AggDailyActivityConfig> {

    private static final Logger log = LoggerFactory.getLogger(AggActivityStreamToDaily.class);

    public static final String BEAN_NAME = "aggActivityStreamToDaily";

    private static final String DAILY_STORE_TABLE_FORMAT = "DailyStream_%s_%s";
    private static final String DAILY_STORE_DELTA_TABLE_FORMAT = "DailyStream_Delta_%s_%s";
    private static final TypeReference<Map<String, Map<String, DimensionMetadata>>> METADATA_MAP_TYPE = new TypeReference<Map<String, Map<String, DimensionMetadata>>>() {
    };

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    private boolean isRematchPA;
    private boolean shortCutMode = false;
    private DataCollection.Version inactive;
    private final Set<String> streamsIncrUpdated = new HashSet<>();
    private Set<String> streamsPerformedDelete = new HashSet<>();
    private final Map<String, String> relinkedDailyStores = new HashMap<>();
    private Set<String> catalogsWithImports;

    @Override
    protected AggDailyActivityConfig configureJob(ActivityStreamSparkStepConfiguration stepConfiguration) {
        if (MapUtils.isEmpty(stepConfiguration.getActivityStreamMap())) {
            return null;
        }
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        catalogsWithImports = getCatalogsWithNewImports();
        isRematchPA = Boolean.TRUE.equals(getObjectFromContext(FULL_REMATCH_PA, Boolean.class));
        Map<String, String> rawStreamTablesAfterDelete = getMapObjectFromContext(RAW_STREAM_TABLE_AFTER_DELETE, String.class, String.class);
        streamsPerformedDelete = MapUtils.isEmpty(rawStreamTablesAfterDelete) ? Collections.emptySet() : rawStreamTablesAfterDelete.keySet();
        Map<String, String> dailyTableNames = getMapObjectFromContext(AGG_DAILY_ACTIVITY_STREAM_TABLE_NAME, String.class, String.class);
        Map<String, String> dailyDeltaTableNames = getMapObjectFromContext(DAILY_ACTIVITY_STREAM_DELTA_TABLE_NAME, String.class, String.class);
        Map<String, Table> rawStreamTableNames = getRawStreamTables();
        Map<String, Table> rawStreamDeltaTables = getTablesFromMapCtxKey(customerSpace.toString(),
                RAW_ACTIVITY_STREAM_DELTA_TABLE_NAME); // unprocessed raw input. will be processed to raw in this step
        Map<String, AtlasStream> streams = stepConfiguration.getActivityStreamMap();
        Set<String> skippedStreamIds = getSkippedStreamIds();
        Set<String> streamsToRelink = getRelinkStreamIds();
        relinkStreams(streamsToRelink);
        Set<AtlasStream> notSkippedStream = streams.values().stream()
                .filter(stream -> !skippedStreamIds.contains(stream.getStreamId()) && !streamsToRelink.contains(stream.getStreamId()))
                .collect(Collectors.toSet());
        AggDailyActivityConfig config = new AggDailyActivityConfig();
        config.incrementalStreams = notSkippedStream.stream()
                .filter(stream -> shouldIncrUpdate(stream) && rawStreamDeltaTables.get(stream.getStreamId()) != null)
                .map(AtlasStream::getStreamId)
                .collect(Collectors.toSet());
        streamsIncrUpdated.addAll(config.incrementalStreams);
        shortCutMode = isShortcutMode(dailyTableNames, dailyDeltaTableNames);
        if (shortCutMode) {
            log.info("Retrieved daily streams {} and delta daily streams {}, going through shortcut mode.",
                    dailyTableNames.values(),
                    dailyDeltaTableNames.values());
            dataCollectionProxy.upsertTablesWithSignatures(configuration.getCustomer(), dailyTableNames, AggregatedActivityStream, inactive);
            return null;
        } else {
            if (isRematchPA) {
                log.info("PA is performing rematch, rebuilding all streams");
            }
            Long paTimestamp = getLongValueFromContext(PA_TIMESTAMP);
            Map<String, String> dailyStoreActiveBatchNames = getActiveDailyStoreTableNames(new ArrayList<>(streams.keySet()));
            // set dimensions
            config.dimensionMetadataMap = getTypedObjectFromContext(STREAM_DIMENSION_METADATA_MAP, METADATA_MAP_TYPE);
            // dimension value -> short ID
            config.dimensionValueIdMap = getMapObjectFromContext(STREAM_DIMENSION_VALUE_ID_MAP, String.class, String.class);
            config.streamReducerMap = new HashMap<>();
            notSkippedStream.forEach(stream -> {
                String streamId = stream.getStreamId();
                Map<String, DimensionCalculator> calculatorMap = new HashMap<>();
                Set<String> hashDimensions = new HashSet<>();
                List<String> additionalDimAttrs = new ArrayList<>(getEntityIds(stream));
                stream.getDimensions().forEach(dimension -> {
                    calculatorMap.put(dimension.getName(), dimension.getCalculator());
                    if (Arrays.asList(HASH, DERIVE).contains(dimension.getGenerator().getOption())) {
                        hashDimensions.add(dimension.getName());
                    } else if (dimension.getUsages() != null && dimension.getUsages().contains(Dedup)) {
                        additionalDimAttrs.add(dimension.getName());
                    }
                });

                config.streamDateAttrs.put(streamId, stream.getDateAttribute());
                config.attrDeriverMap.put(streamId,
                        stream.getAttributeDerivers() == null ? Collections.emptyList() : stream.getAttributeDerivers());
                config.dimensionCalculatorMap.put(streamId, calculatorMap);
                config.hashDimensionMap.put(streamId, hashDimensions);
                config.additionalDimAttrMap.put(streamId, additionalDimAttrs);
                if (stream.getReducer() != null) {
                    config.streamReducerMap.put(streamId, stream.getReducer());
                }
                if (stream.getRetentionDays() != null) {
                    config.streamRetentionDays.put(streamId, stream.getRetentionDays());
                }
            });
            if (notSkippedStream.isEmpty()) {
                log.info("All streams are skipped for daily aggregation, skipping step entirely");
                return null;
            }
            if (CollectionUtils.isNotEmpty(config.incrementalStreams)) {
                log.info("These streams will be going through incremental update: {}", config.incrementalStreams);
            }

            // set input
            List<DataUnit> units = new ArrayList<>();
            SparkIOMetadataWrapper inputMetadata = new SparkIOMetadataWrapper();
            Map<String, SparkIOMetadataWrapper.Partition> detailsMap = new HashMap<>();
            rawStreamTableNames.forEach((streamId, table) -> {
                if (streamsToRelink.contains(streamId)) {
                    return;
                }
                Preconditions.checkArgument(CollectionUtils.size(table.getExtracts()) == 1,
                        String.format("Table %s should only have one extract, got %d", table.getName(),
                                CollectionUtils.size(table.getExtracts())));
                SparkIOMetadataWrapper.Partition details = new SparkIOMetadataWrapper.Partition();
                details.setStartIdx(units.size());
                if (config.incrementalStreams.contains(streamId)) {
                    Table importDelta = rawStreamDeltaTables.get(streamId);
                    if (importDelta == null) {
                        throw new IllegalStateException(String.format("Stream %s is set to incremental update but no import delta table found", streamId));
                    }
                    units.add(importDelta.partitionedToHdfsDataUnit(streamId, Collections.singletonList(StreamDateId.name())));
                    log.info(String.format("Added delta table %s to stream %s for incremental update.", importDelta.getName(), streamId));
                    Table dailyStoreBatch = metadataProxy.getTable(customerSpace.toString(), dailyStoreActiveBatchNames.get(streamId));
                    if (dailyStoreBatch == null) {
                        log.info("Stream {} is set to incremental update but no daily batch table found. Delta table will be taken as new batch store.", streamId);
                        details.setLabels(Collections.singletonList(ActivityMetricsGroupUtils.NO_BATCH));
                    } else {
                        units.add(dailyStoreBatch.partitionedToHdfsDataUnit(streamId, Collections.singletonList(StreamDateId.name())));
                    }
                } else {
                    // stream going through normal rebuild path
                    units.add(table.partitionedToHdfsDataUnit(streamId, Collections.singletonList(StreamDateId.name())));
                }
                detailsMap.put(streamId, details);
            });
            config.setInput(units);
            inputMetadata.setMetadata(detailsMap);
            config.inputMetadata = inputMetadata;
            config.currentEpochMilli = paTimestamp;
            streamsIncrUpdated.addAll(config.incrementalStreams);
            log.info("Agg daily activity stream config = {}", JsonUtils.serialize(config));
            return config;
        }
    }

    private Set<String> getCatalogsWithNewImports() {
        Set<String> catalogs = hasKeyInContext(CATALOG_NEW_IMPORT) ? getSetObjectFromContext(CATALOG_NEW_IMPORT, String.class) : Collections.emptySet();
        log.info("Catalogs with new imports: {}", catalogs);
        return catalogs;
    }

    private void relinkStreams(Set<String> streamsToRelink) {
        if (CollectionUtils.isNotEmpty(streamsToRelink)) {
            log.info("Streams to relink to inactive version: {}", streamsToRelink);
            Map<String, String> signatureTableNames = dataCollectionProxy.getTableNamesWithSignatures(customerSpace.toString(), AggregatedActivityStream, inactive.complement(), new ArrayList<>(streamsToRelink));
            if (MapUtils.isNotEmpty(signatureTableNames)) {
                log.info("Linking existing daily store tables to inactive version: {}", signatureTableNames);
                dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), signatureTableNames, AggregatedActivityStream, inactive);
                relinkedDailyStores.putAll(signatureTableNames);
            }
        }
    }

    private Map<String, Table> getRawStreamTables() {
        if (Boolean.TRUE.equals(getObjectFromContext(ACTIVITY_PARTITION_MIGRATION_PERFORMED, Boolean.class))) {
            return getTablesFromMapCtxKey(customerSpace.toString(), ACTIVITY_MIGRATED_RAW_STREAM);
        }
        return getTablesFromMapCtxKey(customerSpace.toString(), RAW_ACTIVITY_STREAM_TABLE_NAME);
    }

    private Map<String, String> getActiveDailyStoreTableNames(List<String> streamIds) {
        if (Boolean.TRUE.equals(getObjectFromContext(ACTIVITY_PARTITION_MIGRATION_PERFORMED, Boolean.class))) {
            return getMapObjectFromContext(ACTIVITY_MIGRATED_DAILY_STREAM, String.class, String.class);
        }
        return dataCollectionProxy.getTableNamesWithSignatures(customerSpace.toString(), AggregatedActivityStream, inactive.complement(), streamIds);
    }

    private boolean isShortcutMode(Map<String, String> dailyTableNames, Map<String, String> dailyDeltaTableNames) {
        // if any stream needs incremental update, make sure delta table exists
        if (CollectionUtils.isNotEmpty(streamsIncrUpdated)) {
            return allTablesExist(dailyTableNames) && allTablesExist(dailyDeltaTableNames)
                    && tableInHdfs(dailyTableNames, true) && tableInHdfs(dailyDeltaTableNames, true);
        }
        return allTablesExist(dailyTableNames) && tableInHdfs(dailyTableNames, true);
    }

    private boolean shouldIncrUpdate(AtlasStream stream) {
        return !isRematchPA && !configuration.isShouldRebuild()
                && !streamsPerformedDelete.contains(stream.getStreamId())
                && noCatalogHasImport(stream);
    }

    private boolean noCatalogHasImport(AtlasStream stream) {
        return stream.getDimensions().stream().filter(dim -> dim.getCatalog() != null)
                .noneMatch(dim -> catalogsWithImports.contains(dim.getCatalog().getCatalogId()));
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        if (shortCutMode) {
            return;
        }
        SparkIOMetadataWrapper outputMetadata = JsonUtils.deserialize(result.getOutput(), SparkIOMetadataWrapper.class);

        Map<String, HdfsDataUnit> dailyAggUnits = new HashMap<>();
        Map<String, HdfsDataUnit> dailyDeltaUnits = new HashMap<>();
        outputMetadata.getMetadata().forEach((streamId, details) -> {
            int startIdx = details.getStartIdx();
            if (streamsIncrUpdated.contains(streamId)) {
                dailyDeltaUnits.put(streamId, result.getTargets().get(startIdx));
                dailyAggUnits.put(streamId, result.getTargets().get(startIdx + 1));
            } else {
                dailyAggUnits.put(streamId, result.getTargets().get(startIdx));
            }
        });
        log.info("Daily aggregate data units = {}", JsonUtils.serialize(dailyAggUnits));

        // create daily tables
        Map<String, Table> dailyAggTables = dataUnitMapToTableMap(dailyAggUnits, DAILY_STORE_TABLE_FORMAT, null);
        Map<String, Table> dailyDeltaTables = dataUnitMapToTableMap(dailyDeltaUnits, DAILY_STORE_DELTA_TABLE_FORMAT, createRetentionPolicy());

        // add relinked tables to context
        dailyAggTables.putAll(relinkedDailyStores.entrySet().stream().map(entry -> {
            String streamId = entry.getKey();
            String tableName = entry.getValue();
            return Pair.of(streamId, getTableSummary(customerSpace.toString(), tableName));
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue)));

        // link table to role in collection
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        Map<String, String> dailyTableNames = exportToS3AndAddToContext(dailyAggTables,
                AGG_DAILY_ACTIVITY_STREAM_TABLE_NAME);
        Map<String, String> dailyDeltaTableNames = exportToS3AndAddToContext(dailyDeltaTables,
                DAILY_ACTIVITY_STREAM_DELTA_TABLE_NAME);
        dataCollectionProxy.upsertTablesWithSignatures(configuration.getCustomer(), dailyTableNames,
                AggregatedActivityStream, inactive);
        dataCollectionProxy.upsertTablesWithSignatures(configuration.getCustomer(), dailyDeltaTableNames,
                AggregatedActivityStreamDelta, inactive);
        log.info("Daily aggregate activity stream tables = {}, version = {}", dailyTableNames, inactive);
        log.info("Daily aggregate activity stream delta tables = {}, version = {}", dailyDeltaTableNames, inactive);
    }

    private RetentionPolicy createRetentionPolicy() {
        return RetentionPolicyUtil.toRetentionPolicy(1, RetentionPolicyTimeUnit.MONTH);
    }

    private Map<String, Table> dataUnitMapToTableMap(Map<String, HdfsDataUnit> DUMap, String tableNameFmt, RetentionPolicy retentionPolicy) {
        return DUMap.entrySet().stream().map(entry -> {
            String streamId = entry.getKey();
            String tableName = String.format(tableNameFmt, streamId,
                    UuidUtils.shortenUuid(UUID.randomUUID()));
            Table table = dirToTable(tableName, entry.getValue());
            if (retentionPolicy == null) {
                metadataProxy.createTable(configuration.getCustomer(), tableName, table);
            } else {
                metadataProxy.createTempTable(customerSpace.toString(), tableName, table, retentionPolicy);
            }
            return Pair.of(streamId, table);
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    private List<String> getEntityIds(@NotNull AtlasStream stream) {
        if (CollectionUtils.isEmpty(stream.getAggrEntities())) {
            return Collections.emptyList();
        }

        List<String> aggrEntities = stream.getAggrEntities();
        if (aggrEntities.contains(BusinessEntity.Contact.name())) {
            return Arrays.asList(InterfaceName.ContactId.name(), InterfaceName.AccountId.name());
        } else if (aggrEntities.contains(BusinessEntity.Account.name())) {
            return Collections.singletonList(InterfaceName.AccountId.name());
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    protected Class<? extends AbstractSparkJob<AggDailyActivityConfig>> getJobClz() {
        return AggDailyActivityJob.class;
    }

    private Set<String> getSkippedStreamIds() {
        if (!hasKeyInContext(ACTIVITY_STREAMS_SKIP_AGG)) {
            return Collections.emptySet();
        }

        Set<String> skippedStreamIds = getSetObjectFromContext(ACTIVITY_STREAMS_SKIP_AGG, String.class);
        log.info("Stream IDs skipped for daily aggregation = {}", skippedStreamIds);
        return skippedStreamIds;
    }

    private Set<String> getRelinkStreamIds() {
        if (!hasKeyInContext(ACTIVITY_STREAMS_RELINK)) {
            return Collections.emptySet();
        }
        Set<String> streams = getSetObjectFromContext(ACTIVITY_STREAMS_RELINK, String.class);
        log.info("Stream IDs to relink = {}", streams);
        return streams;
    }
}
