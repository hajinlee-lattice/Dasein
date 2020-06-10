package com.latticeengines.cdl.workflow.steps.process;

import static com.latticeengines.domain.exposed.admin.LatticeFeatureFlag.ENABLE_ACCOUNT360;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
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
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.TimeLineJobConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.TimeLineJob;

@Component(GenerateTimeLine.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class GenerateTimeLine extends RunSparkJob<TimeLineSparkStepConfiguration, TimeLineJobConfig> {

    private static Logger log = LoggerFactory.getLogger(GenerateTimeLine.class);
    private static final List<String> RAWSTREAM_PARTITION_KEYS = ImmutableList.of(InterfaceName.__StreamDateId.name());
    private static final String TIMELINE_TABLE_PREFIX = "Timeline_%s";
    private static final String PARTITION_KEY_NAME = InterfaceName.PartitionKey.name();
    private static final String SORT_KEY_NAME = InterfaceName.SortKey.name();
    private static final String SUFFIX = "_TABLE_ROLE";
    static final String BEAN_NAME = "generateTimeline";

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private BatonService batonService;

    @Value("${cdl.processAnalyze.skip.dynamo.publication}")
    private boolean skipPublishDynamo;

    private DataCollection.Version inactive;
    private DataCollection.Version active;

    private boolean needRebuild = false;
    //timelineId -> version
    private Map<String, String> timelineVersionMap;

    //timelineId -> tableRoleTableName
    private Map<String, String> timelineMaterStoreNameMap;

    private DataCollectionStatus dcStatus;

    @Override
    protected Class<? extends AbstractSparkJob<TimeLineJobConfig>> getJobClz() {
        return TimeLineJob.class;
    }

    @Override
    protected TimeLineJobConfig configureJob(TimeLineSparkStepConfiguration stepConfiguration) {
        if (isShortCutMode()) {
            log.info("Already computed this step, skip processing (short-cut mode)");
            return null;
        }
        List<TimeLine> timeLineList = stepConfiguration.getTimeLineList();
        if (CollectionUtils.isEmpty(timeLineList) || MapUtils.isEmpty(configuration.getActivityStreamMap())) {
            log.info("timeline list is null.");
            return null;
        }
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        active = inactive.complement();
        dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        timelineVersionMap = MapUtils.emptyIfNull(dcStatus.getTimelineVersionMap());
        timelineMaterStoreNameMap = dataCollectionProxy.getTableNamesWithSignatures(customerSpace.toString(),
                TableRoleInCollection.TimelineProfile, active, null);
        checkRebuild();
        bumpVersion();
        dcStatus.setTimelineVersionMap(timelineVersionMap);
        putObjectInContext(CDL_COLLECTION_STATUS, dcStatus);
        List<DataUnit> inputs = new ArrayList<>();
        TimeLineJobConfig config = new TimeLineJobConfig();
        config.partitionKey = PARTITION_KEY_NAME;
        config.sortKey = SORT_KEY_NAME;
        config.needRebuild = needRebuild;
        config.tableRoleSuffix = SUFFIX;

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
        if (MapUtils.isNotEmpty(config.timelineRelatedStreamTables) && MapUtils.isNotEmpty(timelineMaterStoreNameMap)) {
            config.timelineRelatedMaterTables =
                    timelineMaterStoreNameMap.entrySet().stream().filter(entry -> config.timelineRelatedMaterTables.keySet().contains(entry.getKey())).map(entry -> Pair.of(entry.getKey(), entry.getValue())).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        } else {
            config.timelineRelatedMaterTables = new HashMap<>();
        }
        toDataUnits(new ArrayList<>(sourceTables.values()), config.rawStreamInputIdx, inputs);
        toDataUnits(new ArrayList<>(config.timelineRelatedMaterTables.values()), config.masterStoreInputIdx, inputs);
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
        config.timelineVersionMap = timelineVersionMap;
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String outputStr = result.getOutput();
        Map<?, ?> rawMap = JsonUtils.deserialize(outputStr, Map.class);
        Map<String, Integer> timelineOutputIdx = JsonUtils.convertMap(rawMap, String.class, Integer.class);
        Preconditions.checkArgument(MapUtils.isNotEmpty(timelineOutputIdx),
                "timeline output index map should not be empty here");
        // timeline -> timeline rawstream table name
        Map<String, String> tableNames = new HashMap<>();
        Map<String, Table> tables = new HashMap<>();
        Map<String, String> mergedMaterStoreNames = new HashMap<>();
        timelineOutputIdx.forEach((timelineId, outputIdx) -> {
            // create table
            String key = String.format(TIMELINE_TABLE_PREFIX, timelineId);
            String name = TableUtils.getFullTableName(key,
                    HashUtils.getCleanedString(UuidUtils.shortenUuid(UUID.randomUUID())));
            Table table = toTable(name, result.getTargets().get(outputIdx));
            metadataProxy.createTable(configuration.getCustomer(), name, table);
            if (timelineId.endsWith(SUFFIX)) {
                int lastIndex = timelineId.lastIndexOf(SUFFIX);
                timelineId = timelineId.substring(0, lastIndex);
                mergedMaterStoreNames.put(timelineId, name);
                exportToS3(table);
            } else {
                tableNames.put(timelineId, name);
                tables.put(timelineId, table);
            }
        });
        if (!needRebuild) {
            timelineMaterStoreNameMap = timelineMaterStoreNameMap.entrySet().stream().map(entry -> {
                if (mergedMaterStoreNames.keySet().contains(entry.getKey())) {
                    return Pair.of(entry.getKey(), mergedMaterStoreNames.get(entry.getKey()));
                } else {
                    return Pair.of(entry.getKey(), entry.getValue());
                }
            }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        } else {
            timelineMaterStoreNameMap = mergedMaterStoreNames;
        }
        log.info("merged timelineMasterStore names = {}.", mergedMaterStoreNames);
        log.info("timeline rawStream table names = {}", tableNames);
        exportToS3AndAddToContext(tables, TIMELINE_RAWTABLE_NAME);
        tableNames.values().forEach(name -> {
            exportToDynamo(name);
            addToListInContext(TEMPORARY_CDL_TABLES, name, String.class);
        });
        dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), timelineMaterStoreNameMap,
                TableRoleInCollection.TimelineProfile, inactive);
    }

    private void exportToDynamo(String tableName) {
        if (shouldPublishDynamo()) {
            String inputPath = metadataProxy.getAvroDir(configuration.getCustomer(), tableName);
            DynamoExportConfig config = new DynamoExportConfig();
            config.setTableName(tableName);
            config.setInputPath(PathUtils.toAvroGlob(inputPath));
            config.setPartitionKey(PARTITION_KEY_NAME);
            config.setSortKey(SORT_KEY_NAME);
            addToListInContext(TIMELINE_RAWTABLES_GOING_TO_DYNAMO, config, DynamoExportConfig.class);
        }
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
                            .filter(timeline -> !timelineVersionMap.containsKey(timeline.getTimelineId()))
                            .collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(newTimelineList)) {
                needRebuild = true;
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
        Map<String, String> timelineRawTableNames = getMapObjectFromContext(TIMELINE_RAWTABLE_NAME,
                String.class, String.class);
        log.info("timeline raw table names = {}", timelineRawTableNames);
        boolean isShortCutMode = allTablesExist(timelineRawTableNames);
        return isShortCutMode;
    }

    private boolean shouldPublishDynamo() {
        boolean hasAccount360 = batonService.isEnabled(customerSpace, ENABLE_ACCOUNT360);
        return !skipPublishDynamo && hasAccount360;
    }
}
