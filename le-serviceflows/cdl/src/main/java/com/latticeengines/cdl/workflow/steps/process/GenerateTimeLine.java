package com.latticeengines.cdl.workflow.steps.process;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.TimelineProfile;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.TimeLineSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.TimeLineJobConfig;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.TimeLineJob;

@Component(GenerateTimeLine.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class GenerateTimeLine extends RunSparkJob<TimeLineSparkStepConfiguration, TimeLineJobConfig> {

    private static Logger log = LoggerFactory.getLogger(GenerateTimeLine.class);
    private static final List<String> RAWSTREAM_PARTITION_KEYS = ImmutableList.of(InterfaceName.StreamDateId.name());
    private static final String PARTITION_KEY_NAME = InterfaceName.PartitionKey.name();
    private static final String SORT_KEY_NAME = InterfaceName.SortKey.name();
    private static final String SUFFIX = String.format("_%s", TableRoleInCollection.TimelineProfile.name());
    static final String BEAN_NAME = "generateTimeline";
    private static final TypeReference<Map<String, Map<String, DimensionMetadata>>> METADATA_MAP_TYPE = new TypeReference<Map<String, Map<String, DimensionMetadata>>>() {
    };

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private ActivityStoreProxy activityStoreProxy;

    @Value("${cdl.job.partition.limit}")
    private Integer numPartitionLimit;

    private DataCollection.Version inactive;
    private DataCollection.Version active;

    private boolean needRebuild = false;
    //timelineId -> version
    private Map<String, String> timelineVersionMap;

    // timelineId -> table name of timeline master store in active version
    private Map<String, String> activeTimelineMasterTableNames;

    private DataCollectionStatus dcStatus;

    @Override
    protected Class<? extends AbstractSparkJob<TimeLineJobConfig>> getJobClz() {
        return TimeLineJob.class;
    }

    @Override
    protected TimeLineJobConfig configureJob(TimeLineSparkStepConfiguration stepConfiguration) {
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        active = inactive.complement();
        if (isShortCutMode()) {
            log.info("Already computed this step, skip processing (short-cut mode)");
            Map<String, String> masterTableNames = getMapObjectFromContext(TIMELINE_MASTER_TABLE_NAME, String.class,
                    String.class);
            log.info("Linking timeline master tables = {} to inactive version = {}", masterTableNames, inactive);
            dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), masterTableNames,
                    TableRoleInCollection.TimelineProfile, inactive);
            return null;
        }
        List<TimeLine> timeLineList = stepConfiguration.getTimeLineList();
        if (CollectionUtils.isEmpty(timeLineList) || MapUtils.isEmpty(configuration.getActivityStreamMap())) {
            log.info("timeline list is null.");
            return null;
        }
        dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (dcStatus.getTimelineRebuildFlag() == null || Boolean.TRUE.equals(dcStatus.getTimelineRebuildFlag())) {
            needRebuild = true;
            dcStatus.setTimelineRebuildFlag(Boolean.FALSE);
        }
        timelineVersionMap = MapUtils.emptyIfNull(dcStatus.getTimelineVersionMap());
        activeTimelineMasterTableNames = MapUtils.emptyIfNull(dataCollectionProxy.getTableNamesWithSignatures(
                customerSpace.toString(), TableRoleInCollection.TimelineProfile, active, null));
        checkRebuild();
        putObjectInContext(TIMELINE_REBUILD, needRebuild);
        bumpVersion();
        dcStatus.setTimelineVersionMap(timelineVersionMap);
        putObjectInContext(CDL_COLLECTION_STATUS, dcStatus);
        List<DataUnit> inputs = new ArrayList<>();
        TimeLineJobConfig config = new TimeLineJobConfig();
        config.partitionKey = PARTITION_KEY_NAME;
        config.sortKey = SORT_KEY_NAME;
        config.needRebuild = needRebuild;
        config.tableRoleSuffix = SUFFIX;
        // set dimensions
        config.dimensionMetadataMap = getTypedObjectFromContext(STREAM_DIMENSION_METADATA_MAP, METADATA_MAP_TYPE);
        if (config.dimensionMetadataMap == null) {
            config.dimensionMetadataMap = activityStoreProxy.getDimensionMetadata(customerSpace.toString(), null, false);
        }
        if (MapUtils.isEmpty(config.dimensionMetadataMap)) {
            log.info("can't find the DimensionMetadata, will skip generate timeline.");
            return null;
        }

        //no atlasStreamTable, will skip
        // streamId -> table name
        Map<String, String> sourceTables = getInputStreamTables();
        if (MapUtils.isEmpty(sourceTables)) {
            log.info("can't find the atlasStream Tables, will skip generate timeline.");
            return null;
        }
        if (MapUtils.isEmpty(configuration.getTemplateToSystemTypeMap())) {
            log.error("can't get templateToSystemTypeMap. will skip generate timeline.");
            return null;
        }
        config.templateToSystemTypeMap =
                configuration.getTemplateToSystemTypeMap().entrySet().stream().map(entry -> Pair.of(entry.getKey(),
                        entry.getValue().name())).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        //timelineId -> (BusinessEntity, streamTableName list)
        config.timelineRelatedStreamTables =
                getTimelineRelatedStreamTables(timeLineList, sourceTables, config.timeLineMap);
        if (MapUtils.isNotEmpty(config.timelineRelatedStreamTables)
                && MapUtils.isNotEmpty(activeTimelineMasterTableNames)) {
            config.timelineRelatedMasterTables =
                    activeTimelineMasterTableNames.entrySet().stream()
                            .filter(entry -> config.timelineRelatedStreamTables.keySet().contains(entry.getKey()))
                            .map(entry -> Pair.of(entry.getKey(), entry.getValue()))
                            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        } else {
            config.timelineRelatedMasterTables = new HashMap<>();
        }
        toDataUnits(new ArrayList<>(sourceTables.values()), config.rawStreamInputIdx, inputs);
        toDataUnits(new ArrayList<>(config.timelineRelatedMasterTables.values()), config.masterStoreInputIdx, inputs);
        config.setInput(inputs);
        Table contactTable = getContactTable();
        if (contactTable != null) {
            config.contactTableIdx = inputs.size();
            inputs.add(contactTable.toHdfsDataUnit("Contact"));
        }
        //TableName -> StreamType
        config.streamTypeWithTableNameMap =
                sourceTables.entrySet().stream()
                        .filter(entry -> (configuration.getActivityStreamMap().get(entry.getKey()) != null && configuration.getActivityStreamMap().get(entry.getKey()).getStreamType() != null))
                        .map(entry -> Pair.of(entry.getValue(),
                                configuration.getActivityStreamMap().get(entry.getKey()).getStreamType().name())).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        //TableName -> StreamId
        config.tableNameToStreamIdMap =
                sourceTables.entrySet().stream().filter(entry -> (configuration.getActivityStreamMap().get(entry.getKey()) != null && configuration.getActivityStreamMap().get(entry.getKey()).getStreamId() != null))
                .map(entry -> Pair.of(entry.getValue(),
                        configuration.getActivityStreamMap().get(entry.getKey()).getStreamId())).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        config.timelineVersionMap = timelineVersionMap;
        config.dimensionMetadataMap =
                config.dimensionMetadataMap.entrySet().stream().filter(entry -> (configuration.getActivityStreamMap().get(entry.getKey()) != null && AtlasStream.StreamType.WebVisit.equals(configuration.getActivityStreamMap().get(entry.getKey()).getStreamType())))
                .map(entry -> Pair.of(entry.getKey(), entry.getValue())).collect(Collectors.toMap(Pair::getKey,
                        Pair::getValue));
        config.numPartitionLimit = numPartitionLimit;
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String outputStr = result.getOutput();
        Map<?, ?> rawMap = JsonUtils.deserialize(outputStr, Map.class);
        Map<String, Integer> timelineOutputIdx = JsonUtils.convertMap(rawMap, String.class, Integer.class);
        Preconditions.checkArgument(MapUtils.isNotEmpty(timelineOutputIdx),
                "timeline output index map should not be empty here");
        // timeline -> timeline diff table
        Map<String, String> tableNames = new HashMap<>();
        Map<String, Table> tables = new HashMap<>();
        Map<String, String> mergedMaterStoreNames = new HashMap<>();
        timelineOutputIdx.forEach((timelineId, outputIdx) -> {
            if (timelineId.endsWith(SUFFIX)) {
                int lastIndex = timelineId.lastIndexOf(SUFFIX);
                timelineId = timelineId.substring(0, lastIndex);
                String masterTableName = timelineId + "_" + NamingUtils.timestamp(TimelineProfile.name());
                Table table = toTable(masterTableName, result.getTargets().get(outputIdx));
                log.info("Create timeline master table {} for timeline ID {}", masterTableName, timelineId);
                metadataProxy.createTable(configuration.getCustomer(), masterTableName, table);
                mergedMaterStoreNames.put(timelineId, masterTableName);
                exportToS3(table);
            } else {
                String diffTableName = timelineId + "_" + NamingUtils.timestamp(TimelineProfile.name() + "Diff");
                Table table = toTable(diffTableName, result.getTargets().get(outputIdx));
                log.info("Create timeline diff table {} for timeline ID {}", diffTableName, timelineId);
                metadataProxy.createTable(configuration.getCustomer(), diffTableName, table);
                tableNames.put(timelineId, diffTableName);
                tables.put(timelineId, table);
            }
        });
        Map<String, String> timelineMasterStore;
        if (!needRebuild) {
            timelineMasterStore = activeTimelineMasterTableNames.entrySet() //
                    .stream() //
                    .map(entry -> Pair.of(entry.getKey(),
                            mergedMaterStoreNames.getOrDefault(entry.getKey(), entry.getValue())))
                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        } else {
            timelineMasterStore = mergedMaterStoreNames;
        }
        log.info("timeline master table names = {}.", timelineMasterStore);
        log.info("timeline diff table names = {}", tableNames);
        exportToS3AndAddToContext(tables, TIMELINE_DIFF_TABLE_NAME);
        putObjectInContext(TIMELINE_MASTER_TABLE_NAME, timelineMasterStore);

        dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), timelineMasterStore,
                TableRoleInCollection.TimelineProfile, inactive);
    }

    private Table getContactTable() {
        Table contactTable = dataCollectionProxy.getTable(customerSpace.toString(), Contact.getBatchStore(), inactive);
        if (contactTable != null) {
            log.info("Using contact batch store {} in inactive version {}", contactTable.getName(), inactive);
            return contactTable;
        }

        contactTable = dataCollectionProxy.getTable(customerSpace.toString(), Contact.getBatchStore(), active);
        if (contactTable != null) {
            log.info("Using contact batch store {} in active version {}", contactTable.getName(), active);
            return contactTable;
        }

        return null;
    }

    private Map<String, String> getAllStreamTables() {
        TableRoleInCollection batchstore = TableRoleInCollection.ConsolidatedActivityStream;
        Map<String, String> tableNames = dataCollectionProxy.getTableNamesWithSignatures(customerSpace.toString(),
                batchstore, inactive, null);
        if (MapUtils.isNotEmpty(tableNames)) {
            log.info("Using activityStream batch store in inactive version {}.", inactive);
            return tableNames;
        }
        tableNames = dataCollectionProxy.getTableNamesWithSignatures(customerSpace.toString(), batchstore, active, null);
        if (MapUtils.isNotEmpty(tableNames)) {
            log.info("Using activityStream batch store in active version {}.", active);
            return tableNames;
        }
        return null;
    }

    private List<HdfsDataUnit> toDataUnits(List<String> tableNames, Map<String, Integer> streamInputIdx, List<DataUnit> inputs) {
        if (CollectionUtils.isEmpty(tableNames)) {
            return Collections.emptyList();
        }
        if (needRebuild) {
            return tableNames.stream() //
                    .map(name -> {
                        streamInputIdx.put(name, inputs.size());
                        return metadataProxy.getTable(configuration.getCustomer(), name);
                    }) //
                    .map(table -> {
                        HdfsDataUnit du = table.partitionedToHdfsDataUnit(null, RAWSTREAM_PARTITION_KEYS);
                        inputs.add(du);
                        return du;
                    }) //
                    .collect(Collectors.toList());
        } else {
            return tableNames.stream() //
                    .map(name -> {
                        streamInputIdx.put(name, inputs.size());
                        return metadataProxy.getTable(configuration.getCustomer(), name);
                    }) //
                    .map(table -> {
                        HdfsDataUnit du = table.toHdfsDataUnit(null);
                        inputs.add(du);
                        return du;
                    }) //
                    .collect(Collectors.toList());
        }
    }

    //timeline -> (entity -> streamTableName list)
    private Map<String, Map<String, Set<String>>> getTimelineRelatedStreamTables(List<TimeLine> timeLineList,
                                                                                 Map<String,
                                                                                         String> streamTables,
                                                                                 Map<String, TimeLine> timeLineMap) {
        Map<String, Map<String, Set<String>>> timelineRelatedStreamTables = new HashMap<>();

        for (TimeLine timeLine : timeLineList) {
            //entity -> streamTableName list
            Map<String, Set<String>> entityStreamSetMap;
            List<AtlasStream> streams =
                    configuration.getActivityStreamMap().entrySet().stream()
                            .filter(entry -> belongToTimeline(entry.getValue(), timeLine)).map(Map.Entry::getValue).collect(Collectors.toList());
            //streamId -> entity
            Map<String, String> streamIdEntityMap = configuration.getActivityStreamMap().entrySet().stream()
                    .filter(entry -> belongToTimeline(entry.getValue(), timeLine)).map(entry -> Pair.of(entry.getKey(),
                            getAtlasStreamEntityInTimeline(entry.getValue(), timeLine))).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
            if (CollectionUtils.isEmpty(streams)) {
                continue;
            }
            entityStreamSetMap = streamTables.entrySet().stream()
                    .filter(entry -> (streamIdEntityMap.keySet().contains(entry.getKey()) && StringUtils.isNotEmpty(streamIdEntityMap.get(entry.getKey()))))
                    .map(entry -> Pair.of(streamIdEntityMap.get(entry.getKey()), entry.getValue()))
                    .collect(Collectors.groupingBy(Pair::getKey, Collectors.mapping(Pair::getValue,
                            Collectors.toSet())));
            entityStreamSetMap =
                    entityStreamSetMap.entrySet().stream().filter(entry -> CollectionUtils.isNotEmpty(entry.getValue())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            if (MapUtils.isEmpty(entityStreamSetMap)) {
                continue;
            }
            timeLineMap.put(timeLine.getTimelineId(), timeLine);
            timelineRelatedStreamTables.put(timeLine.getTimelineId(), entityStreamSetMap);
        }
        log.info("timelineRelatedStreamTables is {}, tenant is {}.", JsonUtils.serialize(timelineRelatedStreamTables)
                , configuration.getCustomer());
        return timelineRelatedStreamTables;
    }

    //if rebuild or have new timeline which didn't create raw stream before, rebuild all timeline.
    private void checkRebuild() {
        if (configuration.isShouldRebuild()) {
            needRebuild = true;
        } else {
            List<TimeLine> newTimelineList =
                    configuration.getTimeLineList().stream()
                            .filter(timeline -> !activeTimelineMasterTableNames.containsKey(timeline.getTimelineId()))
                            .collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(newTimelineList)) {
                String timelineIds = newTimelineList.stream().map(TimeLine::getTimelineId)
                        .collect(Collectors.joining(","));
                log.info("Timeline {} do not have batch store table in active version {}. Rebuilding timeline.",
                        timelineIds, active);
                needRebuild = true;
            } else {
                log.info("All timeline have batch store table in active verion {}. Appending timeline.", active);
            }
        }
    }

    private void bumpVersion() {
        String newVersion = generateNewVersion();
        Map<String, String> newTimelineVersionMap;
        if (needRebuild) {
            newTimelineVersionMap =
                    configuration.getTimeLineList().stream().map(timeLine -> Pair.of(timeLine.getTimelineId(),
                            newVersion)).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        } else {
            newTimelineVersionMap = configuration.getTimeLineList().stream().map(timeLine -> {
                String version = timelineVersionMap.getOrDefault(timeLine.getTimelineId(), newVersion);
                return Pair.of(timeLine.getTimelineId(), version);
            }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        }
        timelineVersionMap = newTimelineVersionMap;
    }

    private boolean belongToTimeline(AtlasStream atlasStream, TimeLine timeLine) {
        if ((timeLine.getStreamTypes() == null || !timeLine.getStreamTypes().contains(atlasStream.getStreamType()))
                && (timeLine.getStreamIds() == null || !timeLine.getStreamIds().contains(atlasStream.getStreamId()))) {
            log.info("stream {} isn't in timeline {}, customerSpace is {}.", atlasStream.getStreamId(),
                    timeLine.getTimelineId(), configuration.getCustomer());
            return false;
        }
        if (atlasStream.getMatchEntities().contains(timeLine.getEntity())) {
            return true;
        } else if (timeLine.getEntity().equalsIgnoreCase(BusinessEntity.Account.name()) && atlasStream.getMatchEntities().contains(Contact.name())) {
            log.info("stream {} in timeline {}, stream entity is {}, timeline entity is {}, customerSpace is {}.",
                    atlasStream.getStreamId(), timeLine.getTimelineId(), atlasStream.getMatchEntities(),
                    timeLine.getEntity(), configuration.getCustomer());
            return true;
        } else {
            log.info("stream {} isn't in timeline {}. stream entity is {}, timeline entity is {}, customerSpace is {}" +
                    ".", atlasStream.getStreamId(), timeLine.getTimelineId(), atlasStream.getMatchEntities(),
                    timeLine.getEntity(), configuration.getCustomer());
            return false;
        }
    }

    private String getAtlasStreamEntityInTimeline(AtlasStream atlasStream, TimeLine timeLine) {
        if (atlasStream.getMatchEntities().contains(timeLine.getEntity())) {
            return timeLine.getEntity();
        } else if (timeLine.getEntity().equalsIgnoreCase(BusinessEntity.Account.name()) && atlasStream.getMatchEntities().contains(Contact.name())) {
            return Contact.name();
        }
        return "";
    }

    private Map<String, String> getInputStreamTables() {
        if (needRebuild) {
            return getAllStreamTables();
        } else {
            return getMapObjectFromContext(ENTITY_MATCH_STREAM_TARGETTABLE, String.class,
                    String.class);
        }
    }

    private String generateNewVersion() {
        return String.valueOf(Instant.now().toEpochMilli());
    }

    private boolean isShortCutMode() {
        Map<String, String> timelineRawTableNames = getMapObjectFromContext(TIMELINE_DIFF_TABLE_NAME,
                String.class, String.class);
        Map<String, String> timelineMasterTableNames = getMapObjectFromContext(TIMELINE_MASTER_TABLE_NAME, String.class,
                String.class);
        log.info("timeline diff table names = {}, master table names = {}", timelineRawTableNames,
                timelineMasterTableNames);
        return allTablesExist(timelineRawTableNames) && allTablesExist(timelineMasterTableNames);
    }
}
