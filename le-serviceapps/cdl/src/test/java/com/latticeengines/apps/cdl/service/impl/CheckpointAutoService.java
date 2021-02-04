package com.latticeengines.apps.cdl.service.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.apps.cdl.end2end.CDLEnd2EndDeploymentTestNGBase;
import com.latticeengines.apps.cdl.entitymgr.ActivityMetricsGroupEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.AtlasStreamEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.CatalogEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.StreamDimensionEntityMgr;
import com.latticeengines.apps.cdl.service.ActivityStoreService;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.apps.core.service.DropBoxService;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroupUtils;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionResponse;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishStatistics;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.TypeConversionUtil;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;

/**
 * this checkpoint will save all system info, including template, datafeedTask..
 * all AtlasStream, catalog, dimension, metadatas if exists
 * will auto zip file and upload this zip to S3.
 */
@Component("checkpointAutoService")
public class CheckpointAutoService extends CheckpointServiceBase {
    private static final Logger log = LoggerFactory.getLogger(CheckpointAutoService.class);

    @Inject
    private S3ImportSystemService s3ImportSystemService;

    @Inject
    private ActivityMetricsGroupEntityMgr activityMetricsGroupEntityMgr;

    @Inject
    private CatalogEntityMgr catalogEntityMgr;

    @Inject
    private StreamDimensionEntityMgr streamDimensionEntityMgr;

    @Inject
    private AtlasStreamEntityMgr atlasStreamEntityMgr;

    @Inject
    private ActivityStoreService activityStoreService;

    @Inject
    private DropBoxService dropBoxService;

    @Inject
    private DataFeedTaskService dataFeedTaskService;

    public void saveCheckpoint(String checkpointName,
                                  String checkpointVersion, String customerSpace) throws IOException {
        String rootDir = "checkpoints/" + checkpointName;
        FileUtils.deleteQuietly(new File(rootDir));
        FileUtils.forceMkdirParent(new File(rootDir));

        downloadHdfsData(checkpointName);

        saveImportSystemIfExists(checkpointName, customerSpace);
        String dimensionMetadataSignature = "";
        DataCollection.Version active = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
        for (DataCollection.Version version : DataCollection.Version.values()) {
            String tablesDir = String.format(TABLE_DIR, LOCAL_CHECKPOINT_DIR, checkpointName, version.name());
            FileUtils.forceMkdir(new File(tablesDir));
            for (TableRoleInCollection role : TableRoleInCollection.values()) {
                saveTableIfExists(role, version, checkpointName);
                if (active.equals(version)) {
                    saveRedshiftTableIfExists(role, version);
                    saveDynamoTableIfExists(checkpointName, role, version);
                }
            }
            if (active.equals(version)) {
                dimensionMetadataSignature = saveDataCollectionStatus(version, checkpointName);
            } else {
                saveDataCollectionStatus(version, checkpointName);
            }
            saveStatsIfExists(version, checkpointName);
        }

        saveCheckPointRedshiftTable(checkpointName, checkpointVersion);
        saveCheckpointVersion(checkpointName);
        saveDataFeedRelationIfExists(checkpointName, customerSpace);
        saveAtlasStreamMetadataIfExists(checkpointName, customerSpace, dimensionMetadataSignature);
        saveEntitySeedTables(checkpointName, checkpointVersion);
        zipCheckpointAndUploadToS3(checkpointName, checkpointVersion);
    }

    private void saveEntitySeedTables(String checkpointName, String checkpointVersion) {
        if (!isEntityMatchEnabled()) {
            log.info("Tenant {} is not an entity match tenant, skip saving entity seed/lookup tables",
                    mainTestTenant.getId());
            return;
        }
        ThreadPoolUtils.doInParallel(Arrays.asList( //
                BusinessEntity.Account.name(), //
                BusinessEntity.Contact.name() //
        ), entity -> saveEntitySeedTable(checkpointName, checkpointVersion, entity));
    }

    private void saveEntitySeedTable(String checkpointName, String checkpointVersion, String entity) {
        log.info("save entitySeedTable to checkpoint {}, checkpointVersion is {}, entity is {}", checkpointName,
                checkpointVersion, entity);
        BumpVersionRequest bumpRequest = new BumpVersionRequest();
        String destTenantId = getCheckPointTenantId(checkpointName, checkpointVersion, entity);
        Tenant destTenant = new Tenant(CustomerSpace.parse(destTenantId).toString());
        bumpRequest.setTenant(destTenant);
        bumpRequest.setEnvironments(Arrays.asList(EntityMatchEnvironment.STAGING, EntityMatchEnvironment.SERVING));
        BumpVersionResponse response = matchProxy.bumpVersion(bumpRequest);
        log.info("after bumpVersion, version is {}. customerSpace is {}.",
                JsonUtils.serialize(response.getVersions()), response.getTenant().getId());

        List<EntityPublishRequest> requests = new ArrayList<>();
        List<Tenant> srcTenants = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(precedingCheckpoints)) {
            srcTenants.addAll(precedingCheckpoints.stream()
                    .map(cp -> new Tenant(CustomerSpace
                            .parse(getCheckPointTenantId(cp, checkpointVersion, entity))
                            .toString()))
                    .collect(Collectors.toList()));
        }
        srcTenants.add(mainTestTenant);

        for (Tenant srcTenant : srcTenants) {
            EntityPublishRequest request = new EntityPublishRequest();
            request.setEntity(entity);
            request.setSrcTenant(srcTenant);

            request.setDestTenant(destTenant);
            request.setDestEnv(EntityMatchEnvironment.STAGING);
            request.setDestTTLEnabled(false);
            request.setBumpupVersion(false);
            request.setNumStagingShards(CDLEnd2EndDeploymentTestNGBase.TEST_NUM_STAGING_SHARDS);
            requests.add(request);
        }
        log.info("Start copying entity match table for {} using request: {}.", entity, JsonUtils.serialize(requests));
        List<EntityPublishStatistics> entityPublishStatistics = matchProxy.publishEntity(requests);
        for (EntityPublishStatistics stats : entityPublishStatistics) {
            log.info("Copied {} {} seeds and {} {} lookup entries to tenant {}", stats.getSeedCount(),
                    entity, //
                    stats.getLookupCount(), entity, //
                    CustomerSpace.shortenCustomerSpace(mainTestTenant.getId()));
        }
    }

    private void saveCheckPointRedshiftTable(String checkpointName, String checkpointVersion) {
        if (MapUtils.isNotEmpty(savedRedshiftTables)) {
            RedshiftService redshiftService = redshiftPartitionService.getBatchUserService(null);
            savedRedshiftTables.forEach((role, table) -> {
                String tgtTable = checkpointRedshiftTableName(checkpointName, role, checkpointVersion);
                if (redshiftService.hasTable(tgtTable)) {
                    redshiftService.dropTable(tgtTable);
                }
                redshiftService.renameTable(table, tgtTable);
            });
        }
    }

    private void saveImportSystemIfExists(String checkpointName, String customerSpace) throws IOException {
        List<S3ImportSystem> importSystemList = s3ImportSystemService.getAllS3ImportSystem(customerSpace);
        if (CollectionUtils.isNotEmpty(importSystemList) && importSystemList.get(0) != null) {
            String jsonFile = String.format(S3IMPORT_SYSTEM_JSONFILE_FORMAT, LOCAL_CHECKPOINT_DIR, checkpointName);
            om.writeValue(new File(jsonFile), importSystemList);
            log.info("Save S3ImportSystems to {}.", jsonFile);
        } else {
            log.info("Failed to get importSystems");
        }
    }

    private void saveDataFeedRelationIfExists(String checkpointName, String customerSpace) throws IOException {
        // Get the workflow ID from the customer space using DataFeed.
        DataFeed dataFeed = dataFeedService.getDefaultDataFeed(customerSpace);
        DataFeedExecution dataFeedExecution = dataFeed.getActiveExecution();
        if (dataFeedExecution != null) {
            Long workflowId = dataFeedExecution.getWorkflowId();
            ExecutionContext executionContext = workflowJobService.getExecutionContextByWorkflowId(customerSpace,
                    workflowId);
            saveWorkflowExecutionContext(checkpointName, executionContext);
        }
        List<DataFeedTask> dataFeedTaskList = dataFeed.getTasks();
        if (CollectionUtils.isNotEmpty(dataFeedTaskList) && dataFeedTaskList.get(0) != null) {
            String dataFeedDir = String.format(DATAFEED_DIR_FORMAT, LOCAL_CHECKPOINT_DIR, checkpointName);
            FileUtils.forceMkdir(new File(dataFeedDir));
            saveDataFeedTaskIfExists(checkpointName, dataFeedTaskList);
        } else {
            log.info("Failed to get dataFeedTasks");
        }
    }

    private void saveWorkflowExecutionContext(String checkpointName, ExecutionContext executionContext) throws IOException {
        if (executionContext == null) {
            log.error("Failed to get execution context");
        } else {
            // Strip out keys we don't need to save. For now, those are the keys that are not all capitals and end
            // in "Configuration" or "Workflow". These can be regenerated when the workflow is restarted from the
            // checkpoint.
            Set<Map.Entry<String, Object>> executionContextMap = executionContext.entrySet();
            executionContextMap.removeIf(mapEntry -> //
                    mapEntry.getKey().endsWith("Configuration") || mapEntry.getKey().endsWith("Workflow"));
            String jsonFile = String.format(EXECUTION_CONTEXT_JSONFILE_FORMAT, LOCAL_CHECKPOINT_DIR, checkpointName);
            om.writeValue(new File(jsonFile), executionContextMap);
            log.info("Save Workflow Execution Context to {}.", jsonFile);
        }
    }

    private void saveDataFeedTaskIfExists(String checkpointName, List<DataFeedTask> dataFeedTasks) throws IOException {
        List<Table> templateTables =
                dataFeedTasks.stream().map(dataFeedTask -> {
                    Table templateTable = dataFeedTask.getImportTemplate();
                    templateTable.setName(dataFeedTask.getFeedType());
                    return templateTable;
                }).collect(Collectors.toList());
        saveTemplateTableIfExists(checkpointName, templateTables);
        String jsonFile = String.format(DATAFEEDTASK_JSONFILE_FORMAT, LOCAL_CHECKPOINT_DIR, checkpointName);
        om.writeValue(new File(jsonFile), dataFeedTasks);
        log.info("Save all dataFeedTasks to file {}.", jsonFile);
    }

    private void saveTemplateTableIfExists(String checkpointName, List<Table> templateTables) throws IOException {
        String jsonFile = String.format(TEMPLATE_JSONFILE_FORMAT, LOCAL_CHECKPOINT_DIR, checkpointName);
        om.writeValue(new File(jsonFile), templateTables);
        log.info("Save all templateTables to file {}.", jsonFile);
    }

    private void saveAtlasStreamMetadataIfExists(String checkpointName, String customerSpace, String signature) throws IOException {
        saveAtlasStreamsAndDimensionIfExists(checkpointName);
        saveCatalogsIfExists(checkpointName);
        saveActivityMetricGroupIfExists(checkpointName);
        saveDimensionMetadatasIfExists(checkpointName, customerSpace, signature);
    }

    private void saveAtlasStreamsAndDimensionIfExists(String checkpointName) throws IOException {
        List<AtlasStream> atlasStreams = atlasStreamEntityMgr.findByTenant(mainTestTenant, true);
        if (CollectionUtils.isNotEmpty(atlasStreams) && atlasStreams.get(0) != null) {
            String localDir = String.format(ATLAS_DIR, LOCAL_CHECKPOINT_DIR, checkpointName);
            FileUtils.forceMkdir(new File(localDir));
            String jsonFile = String.format(ATLASSTREAM_JSONFILE_FORMAT, LOCAL_CHECKPOINT_DIR, checkpointName);
            om.writeValue(new File(jsonFile), atlasStreams);
            log.info("Save all AtlasStreams to file {}.", jsonFile);
        } else {
            log.info("Failed to get AtlasStreams");
        }
    }

    private void saveCatalogsIfExists(String checkpointName) throws IOException {
        List<Catalog> catalogs = catalogEntityMgr.findByTenant(mainTestTenant);
        if (CollectionUtils.isNotEmpty(catalogs) && catalogs.get(0) != null) {
            String jsonFile = String.format(CATALOG_JSONFILE_FORMAT, LOCAL_CHECKPOINT_DIR, checkpointName);
            om.writeValue(new File(jsonFile), catalogs);
            log.info("Save all Catalogs to file {}.", jsonFile);
        } else {
            log.info("Failed to get Catalogs");
        }
    }

    private void saveActivityMetricGroupIfExists(String checkpointName) throws IOException {
        List<ActivityMetricsGroup> metricsGroups = activityMetricsGroupEntityMgr.findByTenant(mainTestTenant);
        if (CollectionUtils.isNotEmpty(metricsGroups) && metricsGroups.get(0) != null) {
            String jsonFile = String.format(METRICGROUP_JSONFILE_FORMAT, LOCAL_CHECKPOINT_DIR, checkpointName);
            Map<String, List<ActivityMetricsGroup>> activityMetricsGroupMap = new HashMap<>();
            for (ActivityMetricsGroup metricsGroup : metricsGroups) {
                String streamId = metricsGroup.getStream().getStreamId();
                List<ActivityMetricsGroup> activityMetricsGroups = activityMetricsGroupMap.get(streamId);
                if (activityMetricsGroups == null) {
                    activityMetricsGroups = new ArrayList<>();
                }
                metricsGroup.setStream(null);
                activityMetricsGroups.add(metricsGroup);
                activityMetricsGroupMap.put(streamId, activityMetricsGroups);
            }
            om.writeValue(new File(jsonFile), activityMetricsGroupMap);
            log.info("Save all ActivityMetricGroups to file {}.", jsonFile);
        } else {
            log.info("Failed to get ActivityMetricGroups");
        }
    }

    private void saveDimensionMetadatasIfExists(String checkpointName, String customerSpace, String signature) throws IOException {
        if (StringUtils.isBlank(signature)) {
            log.info("Can't find signature.");
            return;
        }
        Map<String, Map<String, DimensionMetadata>> dimensionMetadataMap =
                activityStoreService.getDimensionMetadata(customerSpace,
                        signature);
        if (!dimensionMetadataMap.isEmpty()) {
            String jsonFile = String.format(DIMENSION_METADATA_JSONFILE_FORMAT, LOCAL_CHECKPOINT_DIR, checkpointName);
            om.writeValue(new File(jsonFile), dimensionMetadataMap);
            log.info("Save dimensionMetadataMap to file {}.", jsonFile);
        } else {
            log.info("Failed to get dimensionMetadataMap");
        }
    }

    public void zipCheckpointAndUploadToS3(String checkpointName, String checkpointVersion) {
        String rootDir = String.format("%s/%s", LOCAL_CHECKPOINT_DIR, checkpointName);
        try {
            File outputFile = new File(String.format("%s.zip", rootDir));
            log.info("outputFile path is {}.", outputFile.getAbsolutePath());
            if (outputFile.exists()) {
                log.info("{} already exists. Deleting.", outputFile.getAbsolutePath());
                outputFile.delete();
            }
            ZipFile output = new ZipFile(outputFile);
            log.info("Generating {}.", output.getFile().getAbsolutePath());
            String folderToAdd = outputFile.getAbsolutePath().replace(".zip", "/");

            ZipParameters params = new ZipParameters();
            params.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);
            params.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL);
            output.addFolder(folderToAdd, params);
            String targetPath =
                    String.format("%s/%s/%s", S3_CHECKPOINTS_DIR, checkpointVersion, outputFile.getName()).replace("//",
                    "/");
            log.info("targetPath is {}.", targetPath);
            s3Service.uploadLocalFile(S3_BUCKET, targetPath, output.getFile(), true);
        } catch (Throwable t) {
            log.error("Unable to generate final zip file.", t);
        }
    }

    private String saveDataCollectionStatus(DataCollection.Version version, String checkpoint) throws IOException {
        DataCollectionStatus dataCollectionStatus = dataCollectionProxy
                .getOrCreateDataCollectionStatus(mainTestTenant.getId(), version);
        String jsonFile = String.format(DATA_COLLECTION_STATUS_JSONFILE_FORMAT, LOCAL_CHECKPOINT_DIR, checkpoint,
                version.name());
        om.writeValue(new File(jsonFile), dataCollectionStatus);
        log.info("Save DataCollection Status at version {} to {}.", version, jsonFile);
        return dataCollectionStatus.getDimensionMetadataSignature();
    }

    protected void cloneAnUploadTables(String checkpoint, String checkpointVersion) throws IOException {
        dataFeedProxy.getDataFeed(mainTestTenant.getId());
        String[] tenantNames = new String[1];

        DataCollection.Version activeVersion = getCheckpointVersion(checkpoint);
        log.info("activeVersion is {}.", activeVersion);

        Map<String, String> redshiftTablesToClone = new HashMap<>();
        Set<String> uploadedTables = new HashSet<>();
        RedshiftService redshiftService = redshiftPartitionService.getBatchUserService(null);
        String signature = null;
        for (DataCollection.Version version : DataCollection.Version.values()) {
            for (TableRoleInCollection role : TableRoleInCollection.values()) {
                List<Table> tables = parseCheckpointTable(checkpoint, role.name(), version, tenantNames);
                List<String> tableNames = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(tables)) {
                    for (Table table : tables) {
                        if (table != null) {
                            log.info("Creating table {} for {} in version {}, table Path is {}.", table.getName(), role,
                                    version, table.getExtractsDirectory());
                            if (!uploadedTables.contains(table.getName())) {
                                metadataProxy.createTable(mainTestTenant.getId(), table.getName(), table);
                                uploadedTables.add(table.getName());
                            }
                            tableNames.add(table.getName());
                            if (activeVersion.equals(version)) {
                                String redshiftTable = checkpointRedshiftTableName(checkpoint, role, checkpointVersion);
                                if (redshiftService.hasTable(redshiftTable)) {
                                    redshiftTablesToClone.put(redshiftTable, table.getName());
                                }
                                DynamoDataUnit dynamoDataUnit = parseDynamoDataUnit(checkpoint, role.name(), version);
                                if (dynamoDataUnit != null) {
                                    if (StringUtils.isBlank(dynamoDataUnit.getLinkedTable())) {
                                        dynamoDataUnit.setLinkedTable(dynamoDataUnit.getName());
                                    }
                                    if (StringUtils.isBlank(dynamoDataUnit.getLinkedTenant())) {
                                        dynamoDataUnit.setLinkedTenant(dynamoDataUnit.getTenant());
                                    }
                                    dynamoDataUnit.setName(table.getName());
                                    dynamoDataUnit
                                            .setTenant(CustomerSpace.shortenCustomerSpace(mainTestTenant.getId()));
                                    log.info("Creating data unit {}.", JsonUtils.serialize(dynamoDataUnit));
                                    dataUnitProxy.create(mainTestTenant.getId(), dynamoDataUnit);
                                }
                            }
                        }
                    }
                }

                if (CollectionUtils.isNotEmpty(tableNames)) {
                    if (tableNames.size() == 1) {
                        dataCollectionProxy.upsertTable(mainTestTenant.getId(), tableNames.get(0), role, version);
                    } else {
                        dataCollectionProxy.upsertTables(mainTestTenant.getId(), tableNames, role, version);
                    }
                }
            }
            StatisticsContainer statisticsContainer = parseCheckpointStatistics(checkpoint, version);
            if (statisticsContainer != null) {
                dataCollectionProxy.upsertStats(mainTestTenant.getId(), statisticsContainer);
            }
            if (activeVersion.equals(version)) {
                signature = parseSystemInfos(checkpoint, CustomerSpace.parse(mainTestTenant.getId()).toString());

            }
            DataCollectionStatus dataCollectionStatus = parseDataCollectionStatus(checkpoint, version);
            if (dataCollectionStatus != null) {
                if (StringUtils.isNotBlank(signature) && version.equals(activeVersion)) {
                    dataCollectionStatus.setDimensionMetadataSignature(signature);
                }
                dataCollectionProxy.saveOrUpdateDataCollectionStatus(mainTestTenant.getId(), dataCollectionStatus,
                        version);
            }
        }

        String cloneRedshiftMsg = String.format("Clone %d redshift tables", MapUtils.size(redshiftTablesToClone));
        try (PerformanceTimer timer = new PerformanceTimer(cloneRedshiftMsg)) {
            cloneRedshiftTables(redshiftTablesToClone);
        }
        uploadCheckpointHdfs(checkpoint);
        if (copyToS3) {
            uploadCheckpointS3(checkpoint);
        }

        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Active.name());
        resumeDbState();

        copyEntitySeedTables(checkpoint, checkpointVersion);

        dataCollectionProxy.switchVersion(mainTestTenant.getId(), activeVersion);
        log.info("Switch active version to {}.", activeVersion);
    }

    private List<Table> parseCheckpointTable(String checkpoint, String roleName, DataCollection.Version version,
                                             String[] tenantNames) throws IOException {
        String jsonFilePath = String.format(TABLE_JSONFILE_FORMAT, checkpointDir, checkpoint, version.name(),
                roleName);
        log.info("Checking table json file path {}.", jsonFilePath);
        File jsonFile = new File(jsonFilePath);
        if (!jsonFile.exists()) {
            return null;
        }

        log.info("Parse check point {} table {} of version {}.", checkpoint, roleName, version.name());
        List<Table> tables = new ArrayList<>();
        ArrayNode arrNode = (ArrayNode) om.readTree(jsonFile);
        Iterator<JsonNode> iter = arrNode.elements();
        while (iter.hasNext()) {
            JsonNode json = iter.next();
            String tableName = json.get("name").asText();
            String hdfsPath = json.get("extracts_directory").asText();
            if (StringUtils.isBlank(hdfsPath)) {
                hdfsPath = json.get("extracts").get(0).get("path").asText();
                if (hdfsPath.endsWith(".avro") || hdfsPath.endsWith("/")) {
                    hdfsPath = hdfsPath.substring(0, hdfsPath.lastIndexOf("/"));
                }
            } else {
                hdfsPath = hdfsPath.replaceAll(TABLE_DATA_DIR, tableName);
            }
            hdfsPath = hdfsPath.replaceAll(POD_DEFAULT, String.format(POD_PATTERN, podId));
            hdfsPath = hdfsPath.replaceAll(POD_QA, String.format(POD_PATTERN, podId));

            log.info("Parse extract path {}.", hdfsPath);
            Pattern pattern = Pattern.compile(PATH_PATTERN);
            Matcher matcher = pattern.matcher(hdfsPath);
            String str = JsonUtils.serialize(json);
            str = str.replaceAll(POD_DEFAULT, String.format(POD_PATTERN, podId));
            str = str.replaceAll(POD_QA, String.format(POD_PATTERN, podId));
            if (matcher.find()) {
                tenantNames[0] = matcher.group(1);
                log.info("Found tenant name {} in json.", tenantNames[0]);
            } else {
                log.info("Cannot find tenant for {}.", tenantNames[0]);
            }

            if (tenantNames[0] != null) {
                String testTenant = CustomerSpace.parse(mainTestTenant.getId()).getTenantId();
                String hdfsPathSegment1 = hdfsPath.substring(0, hdfsPath.lastIndexOf("/"));
                String hdfsPathSegment2 = hdfsPath.substring(hdfsPath.lastIndexOf("/"));
                String hdfsPathFinal = hdfsPathSegment1.replaceAll(tenantNames[0], testTenant) + hdfsPathSegment2;
                String hdfsPathReplace = hdfsPath.replaceAll(tenantNames[0], testTenant);
                log.info("hdfsPathFinal is {}.", hdfsPathFinal);
                log.info("hdfsPathReplace is {}.", hdfsPathReplace);
                str = str.replaceAll(tenantNames[0], testTenant);
                str = str.replaceAll(hdfsPathReplace, hdfsPathFinal);
            }
            Assert.assertFalse(str.contains(TABLE_DATA_DIR));
            tables.add(JsonUtils.deserialize(str, Table.class));
        }

        return tables;
    }

    private String parseSystemInfos(String checkpoint, String customerSpace) throws IOException {
        updateImportSystemInfos(checkpoint, customerSpace);
        Map<String, DataFeedTask> dataFeedTaskMap = uploadDataFeedTasks(checkpoint, customerSpace);
        if (MapUtils.isEmpty(dataFeedTaskMap)) {
            return null;
        }
        Map<String, Catalog> catalogMap = uploadCatalog(checkpoint, customerSpace, dataFeedTaskMap);
        Map<String,AtlasStream> atlasStreamMap = uploadAtlasStream(checkpoint, customerSpace, dataFeedTaskMap, catalogMap);
        uploadActivityMetricGroup(checkpoint, atlasStreamMap);
        return uploadDimensionMetadata(checkpoint, customerSpace, atlasStreamMap);
    }

    private void updateImportSystemInfos(String checkpoint, String customerSpace)
            throws IOException {
        String jsonFile = String.format(S3IMPORT_SYSTEM_JSONFILE_FORMAT, checkpointDir, checkpoint);
        if (!new File(jsonFile).exists()) {
            log.info("Can't find S3ImportSystemInfos.");
            return;
        }
        List<?> importSystemObjects = JsonUtils.deserialize(new FileInputStream(jsonFile),
                List.class);
        List<S3ImportSystem> importSystemList = JsonUtils.convertList(importSystemObjects, S3ImportSystem.class);
        if (CollectionUtils.isNotEmpty(importSystemList) && importSystemList.get(0) != null) {
            List<S3ImportSystem> existingSystems = s3ImportSystemService.getAllS3ImportSystem(customerSpace);
            List<String> existingSystemNames =
                    new ArrayList<>(existingSystems.stream().map(S3ImportSystem::getName).collect(Collectors.toList()));
            for (S3ImportSystem importSystem : importSystemList) {
                S3ImportSystem newSystem = convertS3ImportSystem(importSystem);
                if (existingSystemNames.contains(newSystem.getName())) {
                    s3ImportSystemService.updateS3ImportSystem(customerSpace, newSystem);
                } else {
                    s3ImportSystemService.createS3ImportSystem(customerSpace, newSystem);
                    dropBoxService.createFolder(customerSpace, newSystem.getName(), null, null);
                }
            }
        } else {
            log.info("S3ImportSystem List is empty.");
        }
    }

    private S3ImportSystem convertS3ImportSystem(S3ImportSystem importSystem) {
        S3ImportSystem newSystem = new S3ImportSystem();
        newSystem.setTenant(mainTestTenant);
        newSystem.setSystemType(importSystem.getSystemType());
        newSystem.setName(importSystem.getName());
        newSystem.setDisplayName(importSystem.getDisplayName());
        newSystem.setPriority(importSystem.getPriority());
        if (StringUtils.isNotEmpty(importSystem.getAccountSystemId())) {
            newSystem.setAccountSystemId(importSystem.getAccountSystemId());
        }
        if (StringUtils.isNotEmpty(importSystem.getContactSystemId())) {
            newSystem.setContactSystemId(importSystem.getContactSystemId());
        }
        if (importSystem.getSecondaryAccountIds() != null) {
            newSystem.setSecondaryAccountIds(importSystem.getSecondaryAccountIds());
        }
        if (importSystem.getSecondaryContactIds() != null) {
            newSystem.setSecondaryContactIds(importSystem.getSecondaryContactIds());
        }
        newSystem.setMapToLatticeAccount(importSystem.isMapToLatticeAccount());
        newSystem.setMapToLatticeContact(importSystem.isMapToLatticeContact());
        return newSystem;
    }

    private Map<String, Table> parseTemplateTable(String checkpoint) throws IOException {
        String jsonFile = String.format(TEMPLATE_JSONFILE_FORMAT, checkpointDir, checkpoint);
        if (!new File(jsonFile).exists()) {
            log.info("Can't find TemplateInfos.");
            return null;
        }
        List<?> templateObjects = JsonUtils.deserialize(new FileInputStream(jsonFile),
                List.class);
        List<Table> templateTables = JsonUtils.convertList(templateObjects, Table.class);
        Map<String, Table> templateTableMaps = new HashMap<>();
        if (CollectionUtils.isNotEmpty(templateTables) && templateTables.get(0) != null) {
            for (Table templateTable : templateTables) {
                templateTableMaps.put(templateTable.getName(), templateTable);
            }
        } else {
            log.info("TemplateTables is empty.");
        }
        return templateTableMaps;
    }

    private Map<String, DataFeedTask> uploadDataFeedTasks(String checkpoint, String customerSpace) throws IOException {
        String jsonFile = String.format(DATAFEEDTASK_JSONFILE_FORMAT, checkpointDir, checkpoint);
        if (!new File(jsonFile).exists()) {
            log.info("Can't find DataFeedTaskInfos.");
            return null;
        }
        List<?> dataFeedTaskObjects = JsonUtils.deserialize(new FileInputStream(jsonFile),
                List.class);
        List<DataFeedTask> dataFeedTaskList = JsonUtils.convertList(dataFeedTaskObjects, DataFeedTask.class);
        //key -> taskUniqueId, value -> dataFeedTask object
        Map<String, DataFeedTask> dataFeedTaskUniqueIdMaps = new HashMap<>();
        Map<String, String> oldDataFeedTaskNameIdMaps = new HashMap<>();
        if (CollectionUtils.isNotEmpty(dataFeedTaskList) && dataFeedTaskList.get(0) != null) {
            Map<String, Table> templateTableMaps = parseTemplateTable(checkpoint);
            if (MapUtils.isEmpty(templateTableMaps)) {
                log.info("Can't find templateTables, skip find DataFeedTask.");
                return dataFeedTaskUniqueIdMaps;
            }
            for (DataFeedTask dataFeedTask : dataFeedTaskList) {
                DataFeedTask newDataFeedTask = new DataFeedTask();
                String templateName = NamingUtils.timestamp(dataFeedTask.getEntity());
                if (templateTableMaps.containsKey(dataFeedTask.getFeedType())) {
                    Table templateTable = templateTableMaps.get(dataFeedTask.getFeedType());
                    templateTable.setName(templateName);
                    newDataFeedTask.setImportTemplate(templateTable);
                } else if (dataFeedTask.getImportTemplate() != null) {
                    newDataFeedTask.setImportTemplate(dataFeedTask.getImportTemplate());
                    newDataFeedTask.getImportTemplate().setName(templateName);
                }
                if (StringUtils.isNotBlank(dataFeedTask.getTaskUniqueName())) {
                    newDataFeedTask.setTaskUniqueName(dataFeedTask.getTaskUniqueName());
                }
                newDataFeedTask.setStatus(dataFeedTask.getStatus());
                newDataFeedTask.setEntity(dataFeedTask.getEntity());
                newDataFeedTask.setFeedType(dataFeedTask.getFeedType());
                newDataFeedTask.setSource(dataFeedTask.getSource());
                newDataFeedTask.setActiveJob("Not specified");
                newDataFeedTask.setSourceConfig("Not specified");
                newDataFeedTask.setStartTime(dataFeedTask.getStartTime());
                newDataFeedTask.setLastImported(dataFeedTask.getLastImported());
                newDataFeedTask.setLastUpdated(dataFeedTask.getLastUpdated());
                newDataFeedTask.setUniqueId(NamingUtils.uuid("DataFeedTask"));
                newDataFeedTask.setDeleted(dataFeedTask.getDeleted());
                newDataFeedTask.setSubType(dataFeedTask.getSubType());
                newDataFeedTask.setTemplateDisplayName(dataFeedTask.getTemplateDisplayName());
                dataFeedTaskService.createDataFeedTask(customerSpace, newDataFeedTask);
                oldDataFeedTaskNameIdMaps.put(newDataFeedTask.getUniqueId(), dataFeedTask.getUniqueId());
            }
            List<DataFeedTask> dataFeedTasks = dataFeedTaskService.getDataFeedTaskByUniqueIds(customerSpace,
                    new ArrayList<>(oldDataFeedTaskNameIdMaps.keySet()));
            for (DataFeedTask existingTask : dataFeedTasks) {
                dataFeedTaskUniqueIdMaps.put(oldDataFeedTaskNameIdMaps.get(existingTask.getUniqueId()),
                        existingTask);
            }
        } else {
            log.info("DataFeedTaskLists is empty");
        }
        return dataFeedTaskUniqueIdMaps;
    }

    private Map<String, AtlasStream> uploadAtlasStream(String checkpoint, String customerSpace,
                                              Map<String, DataFeedTask> dataFeedTaskIdMaps, Map<String, Catalog> catalogMap) throws IOException {
        if (MapUtils.isEmpty(dataFeedTaskIdMaps)) {
            log.info("Can't find dataFeedTaskMaps. skip find AtlasStreams.");
            return null;
        }
        String jsonFile = String.format(ATLASSTREAM_JSONFILE_FORMAT, checkpointDir, checkpoint);
        if (!new File(jsonFile).exists()) {
            log.info("Can't find AtlasStreamInfos.");
            return null;
        }
        List<?> atlasStreamObjects = JsonUtils.deserialize(new FileInputStream(jsonFile),
                List.class);
        List<AtlasStream> atlasStreamList = JsonUtils.convertList(atlasStreamObjects, AtlasStream.class);
        Map<String, AtlasStream> atlasStreamIdMaps = new HashMap<>();
        if (CollectionUtils.isNotEmpty(atlasStreamList) && atlasStreamList.get(0) != null) {
            for (AtlasStream atlasStream : atlasStreamList) {
                AtlasStream newAtlasStream = new AtlasStream();
                newAtlasStream.setTenant(mainTestTenant);
                newAtlasStream.setName(atlasStream.getName());
                newAtlasStream.setDataFeedTask(dataFeedTaskIdMaps.get(atlasStream.getDataFeedTaskUniqueId()));
                newAtlasStream.setMatchEntities(atlasStream.getMatchEntities());
                newAtlasStream.setAggrEntities(atlasStream.getAggrEntities());
                newAtlasStream.setCreated(atlasStream.getCreated());
                newAtlasStream.setReducer(atlasStream.getReducer());
                newAtlasStream.setRetentionDays(atlasStream.getRetentionDays());
                newAtlasStream.setPeriods(atlasStream.getPeriods());
                newAtlasStream.setDateAttribute(atlasStream.getDateAttribute());
                AtlasStream createdStream = activityStoreService.createStream(customerSpace, newAtlasStream);
                atlasStreamIdMaps.put(atlasStream.getStreamId(), createdStream);
                uploadStreamDimension(createdStream, atlasStream.getDimensions(), catalogMap);
            }

        } else {
            log.info("atlasStreamList is empty.");
        }
        return atlasStreamIdMaps;
    }

    private Map<String, Catalog> uploadCatalog(String checkpoint,String customerSpace,
                                               Map<String, DataFeedTask> dataFeedTaskIdMaps) throws IOException {
        if (MapUtils.isEmpty(dataFeedTaskIdMaps)) {
            log.info("Can't find dataFeedTaskMaps. skip find Catalog.");
            return null;
        }
        String jsonFile = String.format(CATALOG_JSONFILE_FORMAT, checkpointDir, checkpoint);
        if (!new File(jsonFile).exists()) {
            log.info("Can't find catalogs.");
            return null;
        }
        List<?> catalogObjects = JsonUtils.deserialize(new FileInputStream(jsonFile), List.class);
        List<Catalog> catalogList = JsonUtils.convertList(catalogObjects, Catalog.class);
        Map<String, Catalog> catalogMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(catalogList) && catalogList.get(0) != null) {
            for (Catalog catalog : catalogList) {
                DataFeedTask dataFeedTask = dataFeedTaskIdMaps.get(catalog.getDataFeedTaskUniqueId());
                Catalog createdCatalog = activityStoreService.createCatalog(customerSpace, catalog.getName(),
                        dataFeedTask.getUniqueId(), catalog.getPrimaryKeyColumn());
                catalogMap.put(catalog.getCatalogId(), createdCatalog);
            }
        } else {
            log.info("catalog list is empty.");
        }
        return catalogMap;
    }

    private void uploadStreamDimension(AtlasStream stream, List<StreamDimension> dimensions, Map<String,
            Catalog> catalogMap) {
        if (CollectionUtils.isEmpty(dimensions) || MapUtils.isEmpty(catalogMap)) {
            log.info("Can't find dimensions/catalogMap. skip upload StreamDimensions.");
            return;
        }
        for (StreamDimension streamDimension : dimensions) {
            StreamDimension newDimension = new StreamDimension();
            newDimension.setCalculator(streamDimension.getCalculator());
            newDimension.setName(streamDimension.getName());
            newDimension.setDisplayName(streamDimension.getDisplayName());
            newDimension.setGenerator(streamDimension.getGenerator());
            newDimension.setStream(stream);
            Catalog oldCatalog = streamDimension.getCatalog();
            if (oldCatalog != null && catalogMap.containsKey(oldCatalog.getCatalogId())) {
                newDimension.setCatalog(catalogMap.get(oldCatalog.getCatalogId()));
            }
            newDimension.setUsages(streamDimension.getUsages());
            newDimension.setTenant(mainTestTenant);
            streamDimensionEntityMgr.create(newDimension);
        }
    }

    private void uploadActivityMetricGroup(String checkpoint, Map<String, AtlasStream> atlasStreamIdMaps) throws IOException {
        if (MapUtils.isEmpty(atlasStreamIdMaps)) {
            log.info("Can't find atlasStreamMap. skip find ActivityMetricGroups.");
            return;
        }
        String jsonFile = String.format(METRICGROUP_JSONFILE_FORMAT, checkpointDir, checkpoint);
        if (!new File(jsonFile).exists()) {
            log.info("Can't find ActivityMetricGroups.");
            return;
        }
        Map<?, List<?>> metricGroupMapObjects = JsonUtils.deserialize(new FileInputStream(jsonFile), Map.class);
        Map<String, List<ActivityMetricsGroup>> metricsGroupMap = JsonUtils.convertMapWithListValue(metricGroupMapObjects, String.class,
                ActivityMetricsGroup.class);
        if (metricsGroupMap != null && !metricsGroupMap.isEmpty()) {
            for (Map.Entry<String, List<ActivityMetricsGroup>> entry : metricsGroupMap.entrySet()) {
                String oldStreamId = entry.getKey();
                List<ActivityMetricsGroup> activityMetricsGroups = entry.getValue();
                if (CollectionUtils.isNotEmpty(activityMetricsGroups) && activityMetricsGroups.get(0) != null) {
                    AtlasStream atlasStream = atlasStreamIdMaps.get(oldStreamId);
                    for (ActivityMetricsGroup activityMetricsGroup : activityMetricsGroups) {
                        ActivityMetricsGroup newMetricGroup = new ActivityMetricsGroup();
                        newMetricGroup.setTenant(mainTestTenant);
                        newMetricGroup.setStream(atlasStream);
                        newMetricGroup.setCategory(activityMetricsGroup.getCategory());
                        newMetricGroup.setGroupName(activityMetricsGroup.getGroupName());
                        newMetricGroup.setGroupId(getGroupId(activityMetricsGroup.getGroupName()));
                        newMetricGroup.setActivityTimeRange(activityMetricsGroup.getActivityTimeRange());
                        if (StringUtils.isNotBlank(activityMetricsGroup.getSubCategoryTmpl().getName())) {
                            newMetricGroup.setSubCategoryTmpl(getTemplate(activityMetricsGroup.getSubCategoryTmpl().getName()));
                        }
                        if (StringUtils.isNotBlank(activityMetricsGroup.getDescriptionTmpl().getName())) {
                            newMetricGroup.setDescriptionTmpl(getTemplate(activityMetricsGroup.getDescriptionTmpl().getName()));
                        }
                        if (StringUtils.isNotBlank(activityMetricsGroup.getDisplayNameTmpl().getName())) {
                            newMetricGroup.setDisplayNameTmpl(activityMetricsGroup.getDisplayNameTmpl());
                        }
                        newMetricGroup.setRollupDimensions(activityMetricsGroup.getRollupDimensions());
                        newMetricGroup.setAggregation(activityMetricsGroup.getAggregation());
                        newMetricGroup.setEntity(activityMetricsGroup.getEntity());
                        if (activityMetricsGroup.getSecondarySubCategoryTmpl() != null && StringUtils.isNotBlank(activityMetricsGroup.getSecondarySubCategoryTmpl().getName())) {
                            newMetricGroup.setSecondarySubCategoryTmpl(getTemplate(activityMetricsGroup.getSecondarySubCategoryTmpl().getName()));
                        }
                        newMetricGroup.setJavaClass(activityMetricsGroup.getJavaClass());
                        newMetricGroup.setNullImputation(activityMetricsGroup.getNullImputation());
                        newMetricGroup.setReducer(activityMetricsGroup.getReducer());
                        log.info("newMetricGroup is {}", JsonUtils.serialize(newMetricGroup));
                        activityMetricsGroupEntityMgr.create(newMetricGroup);
                    }
                }
             }
        } else {
            log.info("ActivityMetricGroup list is empty.");
        }
    }

    private String uploadDimensionMetadata(String checkpoint,
                                         String customerSpace, Map<String, AtlasStream> atlasStreamMap) throws IOException {
        if (MapUtils.isEmpty(atlasStreamMap)) {
            log.info("Can't find atlasStreamMap. skip find dimensionMetadata.");
            return null;
        }
        String jsonFile = String.format(DIMENSION_METADATA_JSONFILE_FORMAT, checkpointDir, checkpoint);
        if (!new File(jsonFile).exists()) {
            log.info("Can't find DimensionMetadatas.");
            return null;
        }
        Map<?, Map> dimensionMetadataMapObjects = JsonUtils.deserialize(new FileInputStream(jsonFile), Map.class);
        Map<String, Map> dimensionMetadataMap =
                JsonUtils.convertMap(dimensionMetadataMapObjects, String.class,
                Map.class);
        Map<String, Map<String, DimensionMetadata>> newDimensionMetadaMap = new HashMap<>();
        if (MapUtils.isNotEmpty(dimensionMetadataMap)) {
            //streamName -> oldStreamId
            Map<String, String> streamNameMap =
                    atlasStreamMap.entrySet().stream().map(entry -> Pair.of(entry.getValue().getName(), entry.getKey())).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
            log.info("streamNameMap is {}.", JsonUtils.serialize(streamNameMap));
            for (Map.Entry<String, Map> entry : dimensionMetadataMap.entrySet()) {
                log.info("dimensionMetadataMap entry is {}.", JsonUtils.serialize(entry));
                String oldStreamName = entry.getKey();
                if (!streamNameMap.containsKey(oldStreamName)) {
                    continue;
                }
                AtlasStream atlasStream = atlasStreamMap.get(streamNameMap.get(oldStreamName));
                Map<String, DimensionMetadata> metadataMap = JsonUtils.convertMap(entry.getValue(), String.class,
                        DimensionMetadata.class);
                newDimensionMetadaMap.put(atlasStream.getStreamId(), metadataMap);
            }
            allocateDimensionIdsAndOverrideMap(customerSpace, newDimensionMetadaMap);
            return activityStoreService.saveDimensionMetadata(customerSpace, null, newDimensionMetadaMap);
        } else {
            log.info("dimensionMetadata is empty.");
            return null;
        }
    }

    // generate short ID for each unique dimension value and update corresponding
    // map
    private void allocateDimensionIdsAndOverrideMap(String customerSpace, Map<String,
                                                    Map<String, DimensionMetadata>> dimensionMetadataMap) {

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
        Map<String, String> valueIdMap = activityStoreService.allocateDimensionId(customerSpace, values);

        log.info("DimensionValueIdMap = {}", valueIdMap);

        // update metadata
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

    private String getGroupId(String groupName) {
        String base = ActivityMetricsGroupUtils.fromGroupNameToGroupIdBase(groupName);
        return activityMetricsGroupEntityMgr.getNextAvailableGroupId(base);
    }
}
