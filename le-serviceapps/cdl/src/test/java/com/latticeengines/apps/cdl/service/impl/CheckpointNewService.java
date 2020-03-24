package com.latticeengines.apps.cdl.service.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.entitymgr.ActivityMetricsGroupEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.AtlasStreamEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.CatalogEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.StreamDimensionEntityMgr;
import com.latticeengines.apps.cdl.service.ActivityStoreService;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.apps.core.service.DropBoxService;
import com.latticeengines.apps.core.util.FeatureFlagUtils;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
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
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.RedshiftDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftPartitionService;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.testframework.exposed.service.TestArtifactService;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;
import com.latticeengines.workflowapi.service.WorkflowJobService;
import com.latticeengines.yarn.exposed.service.EMREnvService;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;

/**
 * this checkpoint will save all system info, including template, datafeedTask..
 * all AtlasStream, catalog, dimension, metadatas if exists
 * will auto zip file and upload this zip to S3.
 */
@Component("checkpointNewService")
public class CheckpointNewService {
    private static final Logger log = LoggerFactory.getLogger(CheckpointNewService.class);

    private static final String S3_CHECKPOINTS_DIR = "le-serviceapps/cdl/end2end/checkpoints";

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private TestArtifactService testArtifactService;

    @Inject
    private RedshiftPartitionService redshiftPartitionService;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private WorkflowJobService workflowJobService;

    @Inject
    private MatchProxy matchProxy;

    @Inject
    private BatonService batonService;

    @Inject
    private EMREnvService emrEnvService;

    @Inject
    private S3Service s3Service;

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

    @Resource(name = "jdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    @Value("${camille.zk.pod.id}")
    private String podId;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${common.test.matchapi.url}")
    private String matchapiHostPort;

    @Value("${aws.customer.s3.bucket}")
    protected String s3CustomerBucket;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    private ObjectMapper om = new ObjectMapper();

    private Tenant mainTestTenant;

    private String checkpointDir;

    // For entity match enabled PA, when saving checkpoint for match lookup/seed
    // table, need to publish all the preceding checkpoints' staging lookup/seed
    // table instead of only current tenant.
    // Reason is in current tenant's staging table, only entries which are
    // touched in match job exist which means the staging table doesn't have
    // complete entity universe for the tenant. Although serving table has
    // complete entity universe, serving table doesn't support scan due to
    // lookup performance concern.
    private List<String> precedingCheckpoints;

    private Map<TableRoleInCollection, String> savedRedshiftTables = new HashMap<>();

    private boolean copyToS3 = false;

    public void setMainTestTenant(Tenant mainTestTenant) {
        this.mainTestTenant = mainTestTenant;
    }

    public void setPrecedingCheckpoints(List<String> precedingCheckpoints) {
        this.precedingCheckpoints = precedingCheckpoints;
    }

    public void enableCopyToS3() {
        copyToS3 = true;
    }

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
            String tablesDir = "checkpoints/" + checkpointName + "/" + version.name() + "/tables";
            FileUtils.forceMkdir(new File(tablesDir));
            for (TableRoleInCollection role : TableRoleInCollection.values()) {
                saveTableIfExists(role, version, checkpointName);
                if (active.equals(version)) {
                    saveRedshiftTableIfExists(role, version);
                    saveDynamoTableIfExists(checkpointName, role, version);
                    dimensionMetadataSignature = saveDataCollectionStatus(version, checkpointName);
                } else {
                    saveDataCollectionStatus(version, checkpointName);
                }
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
            requests.add(request);
        }
        log.info("Start copying entity match table for " + entity + " using request: " + JsonUtils.serialize(requests));
        List<EntityPublishStatistics> entityPublishStatistics = matchProxy.publishEntity(requests);
        for (EntityPublishStatistics stats : entityPublishStatistics) {
            log.info("Copied {} {} seeds and {} {} lookup entries to tenant {}", stats.getSeedCount(),
                    entity, //
                    stats.getLookupCount(), entity, //
                    CustomerSpace.shortenCustomerSpace(mainTestTenant.getId()));
            Assert.assertTrue(stats.getSeedCount() > 0);
            Assert.assertTrue(stats.getLookupCount() > 0);
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
            String jsonFile = String.format("checkpoints/%s/S3ImportSystems.json", checkpointName);
            om.writeValue(new File(jsonFile), importSystemList);
            log.info("Save S3ImportSystems to " + jsonFile);
        } else {
            log.error("Failed to get importSystems");
        }
    }

    private void saveDataFeedRelationIfExists(String checkpointName, String customerSpace) throws IOException {
        // Get the workflow ID from the customer space using DataFeed.
        DataFeed dataFeed = dataFeedService.getDefaultDataFeed(customerSpace);
        DataFeedExecution dataFeedExecution = dataFeed.getActiveExecution();
        Long workflowId = dataFeedExecution.getWorkflowId();
        ExecutionContext executionContext = workflowJobService.getExecutionContextByWorkflowId(customerSpace,
                workflowId);
        saveWorkflowExecutionContext(checkpointName, executionContext);

        List<DataFeedTask> dataFeedTaskList = dataFeed.getTasks();
        if (CollectionUtils.isNotEmpty(dataFeedTaskList) && dataFeedTaskList.get(0) != null) {
            String dataFeedDir = String.format("checkpoints/%s/DataFeed", checkpointName);
            FileUtils.forceMkdir(new File(dataFeedDir));
            saveDataFeedTaskIfExists(checkpointName, dataFeedTaskList);
        } else {
            log.error("Failed to get dataFeedTasks");
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
            for (Map.Entry<String, Object> mapEntry : executionContextMap) {
                if (mapEntry.getKey().endsWith("Configuration") || mapEntry.getKey().endsWith("Workflow")) {
                    executionContextMap.remove(mapEntry);
                }
            }
            String jsonFile = String.format("checkpoints/%s/workflow_execution_context.json", checkpointName);
            om.writeValue(new File(jsonFile), executionContextMap);
            log.info("Save Workflow Execution Context to " + jsonFile);
        }
    }

    private void saveDataFeedTaskIfExists(String checkpointName, List<DataFeedTask> dataFeedTasks) throws IOException {
        List<Table> templateTables =
                new ArrayList<>(dataFeedTasks.stream().map(dataFeedTask -> {
                    Table templateTable = dataFeedTask.getImportTemplate();
                    templateTable.setName(dataFeedTask.getFeedType());
                    return templateTable;
                }).collect(Collectors.toList()));
        saveTemplateTableIfExists(checkpointName, templateTables);
        String jsonFile = String.format("checkpoints/%s/DataFeed/DataFeedTasks.json", checkpointName);
        om.writeValue(new File(jsonFile), dataFeedTasks);
        log.info("Save all dataFeedTasks to file {}.", jsonFile);
    }

    private void saveTemplateTableIfExists(String checkpointName, List<Table> templateTables) throws IOException {
        String jsonFile = String.format("checkpoints/%s/DataFeed/templateTables.json", checkpointName);
        om.writeValue(new File(jsonFile), templateTables);
        log.info("Save all templateTables to file {}.", jsonFile);
    }

    private void saveAtlasStreamMetadataIfExists(String checkpointName, String customerSpace, String signature) throws IOException {
        saveAtlasStreamsIfExists(checkpointName);
        saveAtlasDimensionsIfExists(checkpointName);
        saveCatalogsIfExists(checkpointName);
        saveActivityMetricGroupIfExists(checkpointName);
        saveDimensionMetadatasIfExists(checkpointName, customerSpace, signature);
    }

    private void saveAtlasStreamsIfExists(String checkpointName) throws IOException {
        List<AtlasStream> atlasStreams = atlasStreamEntityMgr.findByTenant(mainTestTenant);
        if (CollectionUtils.isNotEmpty(atlasStreams) && atlasStreams.get(0) == null) {
            String localDir = "checkpoints/" + checkpointName + "/AtlasData";
            FileUtils.deleteQuietly(new File(localDir));
            FileUtils.forceMkdirParent(new File(localDir));
            String jsonFile = String.format("checkpoints/%s/AtlasData/AtlasStreams.json", checkpointName);
            om.writeValue(new File(jsonFile), atlasStreams);
            log.info("Save all AtlasStreams to file {}.", jsonFile);
        } else {
            log.error("Failed to get AtlasStreams");
        }
    }

    private void saveCatalogsIfExists(String checkpointName) throws IOException {
        List<Catalog> catalogs = catalogEntityMgr.findByTenant(mainTestTenant);
        if (CollectionUtils.isNotEmpty(catalogs) && catalogs.get(0) == null) {
            String jsonFile = String.format("checkpoints/%s/AtlasData/Catalogs.json", checkpointName);
            om.writeValue(new File(jsonFile), catalogs);
            log.info("Save all Catalogs to file {}.", jsonFile);
        } else {
            log.error("Failed to get Catalogs");
        }
    }

    private void saveAtlasDimensionsIfExists(String checkpointName) throws IOException {
        List<StreamDimension> dimensions = streamDimensionEntityMgr.findByTenant(mainTestTenant);
        if (CollectionUtils.isNotEmpty(dimensions) && dimensions.get(0) == null) {
            String jsonFile = String.format("checkpoints/%s/AtlasData/StreamDimensions.json", checkpointName);
            Map<String, StreamDimension> dimensionMap = new HashMap<>();
            for (StreamDimension dimension : dimensions) {
                dimensionMap.put(dimension.getStream().getStreamId(), dimension);
            }
            om.writeValue(new File(jsonFile), dimensionMap);
            log.info("Save all StreamDimensions to file {}.", jsonFile);
        } else {
            log.error("Failed to get StreamDimensions");
        }
    }

    private void saveActivityMetricGroupIfExists(String checkpointName) throws IOException {
        List<ActivityMetricsGroup> metricsGroups = activityMetricsGroupEntityMgr.findByTenant(mainTestTenant);
        if (CollectionUtils.isNotEmpty(metricsGroups) && metricsGroups.get(0) == null) {
            String jsonFile = String.format("checkpoints/%s/AtlasData/ActivityMetricGroups.json", checkpointName);
            Map<String, List<ActivityMetricsGroup>> activityMetricsGroupMap = new HashMap<>();
            for (ActivityMetricsGroup metricsGroup : metricsGroups) {
                String streamId = metricsGroup.getStream().getStreamId();
                List<ActivityMetricsGroup> activityMetricsGroups = activityMetricsGroupMap.get(streamId);
                if (activityMetricsGroups == null) {
                    activityMetricsGroups = new ArrayList<>();
                }
                activityMetricsGroups.add(metricsGroup);
                activityMetricsGroupMap.put(streamId, activityMetricsGroups);
            }
            om.writeValue(new File(jsonFile), activityMetricsGroupMap);
            log.info("Save all ActivityMetricGroups to file {}.", jsonFile);
        } else {
            log.error("Failed to get ActivityMetricGroups");
        }
    }

    private void saveDimensionMetadatasIfExists(String checkpointName, String customerSpace, String signature) throws IOException {
        Map<String, Map<String, DimensionMetadata>> dimensionMetadataMap =
                activityStoreService.getDimensionMetadata(customerSpace,
                        signature);
        if (!dimensionMetadataMap.isEmpty()) {
            String jsonFile = String.format("checkpoints/%s/AtlasData/DimensionMetadatas.json", checkpointName);
            om.writeValue(new File(jsonFile), dimensionMetadataMap);
            log.info("Save dimensionMetadataMap to file {}.", jsonFile);
        } else {
            log.error("Failed to get dimensionMetadataMap");
        }
    }

    private void zipCheckpointAndUploadToS3(String checkpointName, String checkpointVersion) {
        String rootDir = "checkpoints/" + checkpointName;
        try {
            File outputFile = new File(String.format("%s.zip", rootDir));
            log.info("outputFile path is {}.", outputFile.getAbsolutePath());
            if (outputFile.exists()) {
                log.info(outputFile.getAbsolutePath() + " already exists. Deleting.");
                outputFile.delete();
            }
            ZipFile output = new ZipFile(outputFile);
            log.info("Generating {}.", output.getFile().getAbsolutePath());
            String folderToAdd = outputFile.getAbsolutePath().replace(".zip", "/");

            ZipParameters params = new ZipParameters();
            params.setIncludeRootFolder(false);
            params.setRootFolderInZip("");
            output.addFolder(folderToAdd, params);
            testArtifactService.uploadFileToS3(S3_CHECKPOINTS_DIR, checkpointVersion, output.getFile());
        } catch (Throwable t) {
            log.error("Unable to generate final zip file.", t);
        }
    }

    private void saveCheckpointVersion(String checkpoint) throws IOException {
        String versionFile = "checkpoints/" + checkpoint + "/_VERSION_";
        DataCollection.Version version = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
        FileUtils.write(new File(versionFile), version.name(), "UTF-8");
    }

    private void downloadHdfsData(String checkpoint) throws IOException {
        String localDir = "checkpoints/" + checkpoint + "/hdfs/Data";
        FileUtils.deleteQuietly(new File(localDir));
        FileUtils.forceMkdirParent(new File(localDir));
        CustomerSpace cs = CustomerSpace.parse(mainTestTenant.getId());
        String targetPath = PathBuilder.buildCustomerSpacePath(podId, cs).append("Data").toString();
        HdfsUtils.copyHdfsToLocal(yarnConfiguration, targetPath, localDir);
        log.info("Downloaded HDFS path " + targetPath + " to " + localDir);
        Collection<File> crcFiles = FileUtils.listFiles(new File(localDir), new String[] { "crc" }, true);
        crcFiles.forEach(FileUtils::deleteQuietly);
    }

    private void saveTableIfExists(TableRoleInCollection role, DataCollection.Version version, String checkpoint)
            throws IOException {
        List<Table> tables = new ArrayList<>();
        if (role == TableRoleInCollection.ConsolidatedPeriodTransaction) {
            tables = dataCollectionProxy.getTables(mainTestTenant.getId(), role, version);
        } else {
            tables.add(dataCollectionProxy.getTable(mainTestTenant.getId(), role, version));
        }
        if (CollectionUtils.isNotEmpty(tables) && tables.get(0) != null) {
            String jsonFile = String.format("checkpoints/%s/%s/tables/%s.json", checkpoint, version.name(),
                    role.name());
            om.writeValue(new File(jsonFile), tables);
            log.info("Save " + role + " at version " + version + " to " + jsonFile);
        } else {
            log.info("There is no " + role + " table at version " + version);
        }
    }

    private void saveRedshiftTableIfExists(TableRoleInCollection role, DataCollection.Version version) {
        String tableName = dataCollectionProxy.getTableName(mainTestTenant.getId(), role, version);
        RedshiftService redshiftService = redshiftPartitionService.getBatchUserService(null);
        if (StringUtils.isNotBlank(tableName)) {
            List<String> redshiftTables = redshiftService.getTables(tableName);
            if (CollectionUtils.isNotEmpty(redshiftTables)) {
                if (redshiftTables.size() != 1) {
                    throw new IllegalStateException("There are " + redshiftTables.size()
                            + " redshift tables prefixed by " + tableName + ": " + redshiftTables);
                }
                String oldTable = redshiftTables.get(0);
                String newTable = String.format("%s_%s", TestFrameworkUtils.generateTenantName(), role.name());
                newTable = NamingUtils.timestamp(newTable);
                redshiftService.dropTable(newTable);
                redshiftService.renameTable(oldTable, newTable);
                savedRedshiftTables.put(role, newTable);
            }
        }
    }

    private void saveDynamoTableIfExists(String checkpoint, TableRoleInCollection role, DataCollection.Version version)
            throws IOException {
        String tableName = dataCollectionProxy.getTableName(mainTestTenant.getId(), role, version);
        if (StringUtils.isNotBlank(tableName)) {
            DataUnit dataUnit = dataUnitProxy.getByNameAndType(mainTestTenant.getId(), tableName,
                    DataUnit.StorageType.Dynamo);
            if (dataUnit != null) {
                DynamoDataUnit dynamoDataUnit = (DynamoDataUnit) dataUnit;
                String jsonFile = String.format("checkpoints/%s/%s/%s_Dynamo.json", checkpoint, version.name(),
                        role.name());
                om.writeValue(new File(jsonFile), dynamoDataUnit);
                log.info("Save DynamoDataUnit for " + role + " at version " + version + " to " + jsonFile);
            }
        }
    }

    private void saveStatsIfExists(DataCollection.Version version, String checkpoint) throws IOException {
        StatisticsContainer statisticsContainer = dataCollectionProxy.getStats(mainTestTenant.getId(), version);
        if (statisticsContainer != null) {
            String jsonFile = String.format("checkpoints/%s/%s/stats_container.json", checkpoint, version.name());
            om.writeValue(new File(jsonFile), statisticsContainer);
            log.info("Save stats at version " + version + " to " + jsonFile);
        } else {
            log.info("There is no stats at version " + version);
        }
    }

    private String saveDataCollectionStatus(DataCollection.Version version, String checkpoint) throws IOException {
        DataCollectionStatus dataCollectionStatus = dataCollectionProxy
                .getOrCreateDataCollectionStatus(mainTestTenant.getId(), version);
        String jsonFile = String.format("checkpoints/%s/%s/data_collection_status.json", checkpoint, version.name());
        om.writeValue(new File(jsonFile), dataCollectionStatus);
        log.info("Save DataCollection Status at version " + version + " to " + jsonFile);
        return dataCollectionStatus.getDimensionMetadataSignature();
    }

    private String checkpointRedshiftTableName(String checkpoint, TableRoleInCollection role,
                                               String checkpointVersion) {
        return String.format("cdlend2end_%s_%s_%s", checkpoint, role.name(), checkpointVersion);
    }

    public static String getCheckPointTenantId(String checkpoint, String checkpointVersion, String entity) {
        return "cdlend2end_" + checkpoint + "_" + entity.toLowerCase() + "_" + checkpointVersion;
    }

    @VisibleForTesting
    boolean isEntityMatchEnabled() {
        FeatureFlagValueMap flags = batonService.getFeatureFlags(CustomerSpace.parse(mainTestTenant.getId()));
        boolean emEnabled = FeatureFlagUtils.isEntityMatchEnabled(flags);
        log.info("Feature flags = {}, isEntityMatch={}", flags, emEnabled);
        return emEnabled;
    }

    public void resumeCheckpoint(String checkpoint, int checkpointVersion) throws IOException {
        resumeCheckpoint(checkpoint, String.valueOf(checkpointVersion));
    }

    public void resumeCheckpoint(String checkpoint, String checkpointVersion) throws IOException {
        unzipCheckpoint(checkpoint, checkpointVersion);
        cloneAnUploadTables(checkpoint, checkpointVersion);
        updateDataCloudBuildNumber();
    }

    private void cloneAnUploadTables(String checkpoint, String checkpointVersion) throws IOException {
        dataFeedProxy.getDataFeed(mainTestTenant.getId());
        String[] tenantNames = new String[1];

        DataCollection.Version activeVersion = getCheckpointVersion(checkpoint);

        Map<String, String> redshiftTablesToClone = new HashMap<>();
        Set<String> uploadedTables = new HashSet<>();
        RedshiftService redshiftService = redshiftPartitionService.getBatchUserService(null);
        for (DataCollection.Version version : DataCollection.Version.values()) {
            for (TableRoleInCollection role : TableRoleInCollection.values()) {
                List<Table> tables = parseCheckpointTable(checkpoint, role.name(), version, tenantNames);
                List<String> tableNames = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(tables)) {
                    for (Table table : tables) {
                        if (table != null) {
                            log.info("Creating table " + table.getName() + " for " + role + " in version " + version);
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
                                    log.info("Creating data unit " + JsonUtils.serialize(dynamoDataUnit));
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
            String signature = parseSystemInfos(checkpoint, CustomerSpace.parse(mainTestTenant.getId()).toString());
            DataCollectionStatus dataCollectionStatus = parseDataCollectionStatus(checkpoint, version);
            if (dataCollectionStatus != null) {
                if (StringUtils.isNotBlank(signature)) {
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
        log.info("Switch active version to " + activeVersion);
    }

    private void cloneRedshiftTables(Map<String, String> redshiftTablesToClone) {
        if (MapUtils.isNotEmpty(redshiftTablesToClone)) {
            RedshiftService redshiftService = redshiftPartitionService.getBatchUserService(null);
            ThreadPoolUtils.doInParallel(redshiftTablesToClone.entrySet(), entry -> {
                String src = entry.getKey();
                String tgt = entry.getValue();
                String msg = "Clone redshift table " + src + " to " + tgt;
                try (PerformanceTimer timer = new PerformanceTimer(msg)) {
                    redshiftService.cloneTable(src, tgt);
                    RedshiftDataUnit dataUnit = new RedshiftDataUnit();
                    dataUnit.setTenant(CustomerSpace.shortenCustomerSpace(mainTestTenant.getId()));
                    dataUnit.setName(tgt);
                    dataUnit.setRedshiftTable(tgt.toLowerCase());
                    dataUnit.setClusterPartition(redshiftPartitionService.getDefaultPartition());
                    dataUnitProxy.create(mainTestTenant.getId(), dataUnit);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to clone redshift table.", e);
                }
            });
        }
    }

    private void unzipCheckpoint(String checkpoint, String version) throws IOException {
        checkpointDir = CustomerSpace.parse(mainTestTenant.getId()).getTenantId();
        File downloadedFile = testArtifactService.downloadTestArtifact(S3_CHECKPOINTS_DIR, version,
                checkpoint + ".zip");
        String zipFilePath = downloadedFile.getPath();
        try {
            ZipFile zipFile = new ZipFile(zipFilePath);
            zipFile.extractAll(checkpointDir);
        } catch (ZipException e) {
            throw new IOException("Failed to unzip checkpoint archive " + zipFilePath, e);
        }
        log.info(String.format("Unzip checkpoint archive %s to local dir %s", zipFilePath, checkpointDir));
    }

    @SuppressWarnings("unused")
    private void verifyStatistics() {
        StatisticsContainer statisticsContainer = dataCollectionProxy.getStats(mainTestTenant.getId());
        Assert.assertNotNull(statisticsContainer);
        Map<String, StatsCube> cubeMap = statisticsContainer.getStatsCubes();
        cubeMap.values().forEach(cube -> cube.getStatistics().forEach(this::verifyAttrStats));
    }

    private void verifyAttrStats(String attrName, AttributeStats attributeStats) {
        Assert.assertNotNull(attributeStats.getNonNullCount());
        Assert.assertTrue(attributeStats.getNonNullCount() >= 0);
        Buckets buckets = attributeStats.getBuckets();
        if (buckets != null) {
            Assert.assertNotNull(buckets.getType());
            Assert.assertFalse(buckets.getBucketList() == null || buckets.getBucketList().isEmpty(),
                    "Bucket list for " + attrName + " is empty.");
            Long sum = buckets.getBucketList().stream().mapToLong(Bucket::getCount).sum();
            Assert.assertEquals(sum, attributeStats.getNonNullCount());
        }
    }

    private void uploadCheckpointHdfs(String checkpoint) throws IOException {
        log.info("Start uploading checkpoint " + checkpoint + " to hdfs.");
        String localDir = checkpointDir + "/" + checkpoint + "/hdfs/Data";
        CustomerSpace cs = CustomerSpace.parse(mainTestTenant.getId());
        String targetPath = PathBuilder.buildDataPath(podId, cs).toString();
        HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, localDir, targetPath);
        log.info("Upload checkpoint to hdfs path " + targetPath);
    }

    private void uploadCheckpointS3(String checkpoint) {
        log.info("Start uploading checkpoint " + checkpoint + " to S3.");
        CustomerSpace cs = CustomerSpace.parse(mainTestTenant.getId());
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(useEmr);
        String s3Prefix = pathBuilder.getS3AtlasDataPrefix(s3CustomerBucket, cs.getTenantId());
        if (s3Service.isNonEmptyDirectory(s3CustomerBucket, s3Prefix)) {
            log.info("S3 prefix {} already exists, delete first.", s3Prefix);
            s3Service.cleanupPrefix(s3CustomerBucket, s3Prefix);
        }
        String localDir = checkpointDir + "/" + checkpoint + "/hdfs/Data";
        s3Service.uploadLocalDirectory(s3CustomerBucket, s3Prefix, localDir, true);
    }

    private DataCollection.Version getCheckpointVersion(String checkpoint) throws IOException {
        String versionFile = String.format("%s/%s/_VERSION_", checkpointDir, checkpoint);
        String version = FileUtils.readFileToString(new File(versionFile), "UTF-8").trim();
        return DataCollection.Version.valueOf(version);
    }

    private DynamoDataUnit parseDynamoDataUnit(String checkpoint, String roleName, DataCollection.Version version)
            throws IOException {
        DynamoDataUnit dynamoDataUnit = null;
        String jsonFilePath = String.format("%s/%s/%s/%s_Dynamo.json", checkpointDir, checkpoint, version.name(),
                roleName);
        File jsonFile = new File(jsonFilePath);
        if (jsonFile.exists()) {
            log.info("Found dynamo data unit for " + roleName + " in " + version);
            dynamoDataUnit = om.readValue(jsonFile, DynamoDataUnit.class);
        }
        return dynamoDataUnit;
    }

    private List<Table> parseCheckpointTable(String checkpoint, String roleName, DataCollection.Version version,
                                             String[] tenantNames) throws IOException {
        String jsonFilePath = String.format("%s/%s/%s/tables/%s.json", checkpointDir, checkpoint, version.name(),
                roleName);
        log.info("Checking table json file path " + jsonFilePath);
        File jsonFile = new File(jsonFilePath);
        if (!jsonFile.exists()) {
            return null;
        }

        log.info("Parse check point " + checkpoint + " table " + roleName + " of version " + version.name());
        List<Table> tables = new ArrayList<>();
        ArrayNode arrNode = (ArrayNode) om.readTree(jsonFile);
        Iterator<JsonNode> iter = arrNode.elements();
        while (iter.hasNext()) {
            JsonNode json = iter.next();
            String hdfsPath = json.get("extracts_directory").asText();
            if (StringUtils.isBlank(hdfsPath)) {
                hdfsPath = json.get("extracts").get(0).get("path").asText();
                if (hdfsPath.endsWith(".avro") || hdfsPath.endsWith("/")) {
                    hdfsPath = hdfsPath.substring(0, hdfsPath.lastIndexOf("/"));
                }
            }
            log.info("Parse extract path " + hdfsPath);
            Pattern pattern = Pattern.compile("/Contracts/(.*)/Tenants/");
            Matcher matcher = pattern.matcher(hdfsPath);
            String str = JsonUtils.serialize(json);
            str = str.replaceAll("/Pods/Default/", "/Pods/" + podId + "/");
            str = str.replaceAll("/Pods/QA/", "/Pods/" + podId + "/");
            if (matcher.find()) {
                tenantNames[0] = matcher.group(1);
                log.info("Found tenant name " + tenantNames[0] + " in json.");
            } else {
                log.info("Cannot find tenant for " + tenantNames[0]);
            }

            if (tenantNames[0] != null) {
                String testTenant = CustomerSpace.parse(mainTestTenant.getId()).getTenantId();
                String hdfsPathSegment1 = hdfsPath.substring(0, hdfsPath.lastIndexOf("/"));
                String hdfsPathSegment2 = hdfsPath.substring(hdfsPath.lastIndexOf("/"));
                if (hdfsPathSegment2.contains(tenantNames[0])) {
                    String hdfsPathIntermediatePattern = hdfsPathSegment1.replaceAll(tenantNames[0], testTenant) //
                            + "/$$TABLE_DATA_DIR$$";
                    String hdfsPathFinal = hdfsPathSegment1.replaceAll(tenantNames[0], testTenant) + hdfsPathSegment2;
                    str = str.replaceAll(hdfsPath, hdfsPathIntermediatePattern);
                    str = str.replaceAll(tenantNames[0], testTenant);
                    str = str.replaceAll(hdfsPathIntermediatePattern, hdfsPathFinal);
                } else {
                    str = str.replaceAll(tenantNames[0], testTenant);
                }
            }
            tables.add(JsonUtils.deserialize(str, Table.class));
        }

        return tables;
    }

    private StatisticsContainer parseCheckpointStatistics(String checkpoint, DataCollection.Version version)
            throws IOException {
        String statsFile = String.format("%s/%s/%s/stats_container.json", checkpointDir, checkpoint, version.name());
        if (!new File(statsFile).exists()) {
            return null;
        }
        StatisticsContainer statisticsContainer = JsonUtils.deserialize(new FileInputStream(statsFile),
                StatisticsContainer.class);
        statisticsContainer.setName(NamingUtils.timestamp("Stats"));
        return statisticsContainer;
    }

    private DataCollectionStatus parseDataCollectionStatus(String checkpoint, DataCollection.Version version)
            throws IOException {
        String jsonFile = String.format("%s/%s/%s/data_collection_status.json", checkpointDir, checkpoint,
                version.name());
        if (!new File(jsonFile).exists()) {
            return null;
        }
        DataCollectionStatus status = JsonUtils.deserialize(new FileInputStream(jsonFile), DataCollectionStatus.class);
        status.setRedshiftPartition(redshiftPartitionService.getDefaultPartition());
        return status;
    }

    public void cleanup() {
        if (StringUtils.isNotBlank(checkpointDir)) {
            FileUtils.deleteQuietly(new File(checkpointDir));
        }
    }

    private void resumeDbState() {
        log.info("Resuming DB state.");
        Long tenantPid = getTenantPid();
        Long feedPid = getDataFeedPid(tenantPid);
        Long workflowId = createFakeWorkflow(tenantPid);
        Long executionId = createExecution(feedPid, workflowId);
        updateActiveExecution(feedPid, executionId);

        createFakeWorkflow(tenantPid);
    }

    private Long getTenantPid() {
        String sql = "SELECT `TENANT_PID` FROM `TENANT` WHERE ";
        sql += String.format("`TENANT_ID` = '%s'", mainTestTenant.getId());
        return jdbcTemplate.queryForObject(sql, Long.class);
    }

    private Long createFakeWorkflow(long tenantPid) {
        int rand = new Random(System.currentTimeMillis()).nextInt(10000);
        String appId = String.format("application_%d_%04d", System.currentTimeMillis(), rand);

        String sql = "INSERT INTO `WORKFLOW_JOB` ";
        sql += "(`TENANT_ID`, `FK_TENANT_ID`, `USER_ID`, `APPLICATION_ID`, `INPUT_CONTEXT`, `START_TIME`) VALUES ";
        sql += String.format("(%d, %d, 'DEFAULT_USER', '%s', '{}', NOW())", tenantPid, tenantPid, appId);
        jdbcTemplate.execute(sql);

        sql = "SELECT `PID` FROM `WORKFLOW_JOB` WHERE ";
        sql += String.format("`TENANT_ID` = %d", tenantPid);
        sql += String.format(" AND `APPLICATION_ID` = '%s'", appId);
        long pid = jdbcTemplate.queryForObject(sql, Long.class);
        log.info("Created a fake workflow " + pid);
        return pid;
    }

    private Long createExecution(long feedPid, long workflowPid) {
        String sql = "INSERT INTO `DATAFEED_EXECUTION` ";
        sql += "(`FK_FEED_ID`, `WORKFLOW_ID`, `STATUS`, `JOB_TYPE`) VALUES ";
        sql += String.format("(%d, %d, '%s', '%s')", feedPid, workflowPid, DataFeedExecution.Status.Completed.name(),
                DataFeedExecutionJobType.PA.name());
        jdbcTemplate.execute(sql);

        sql = "SELECT `PID` FROM `DATAFEED_EXECUTION` WHERE `FK_FEED_ID` = " + feedPid;
        long pid = jdbcTemplate.queryForObject(sql, Long.class);
        log.info("Created a fake execution " + pid);
        return pid;
    }

    private Long getDataFeedPid(long tenantPid) {
        String sql = "SELECT `PID` FROM `DATAFEED` WHERE ";
        sql += String.format("`FK_TENANT_ID` = %d", tenantPid);
        return jdbcTemplate.queryForObject(sql, Long.class);
    }

    private void updateActiveExecution(Long feedPid, Long executionId) {
        String sql = "UPDATE `DATAFEED` ";
        sql += String.format("SET `ACTIVE_EXECUTION` = %d ", executionId);
        sql += String.format("WHERE `PID` = %d", feedPid);
        jdbcTemplate.execute(sql);
        log.info("Set execution " + executionId + " as active execution of data feed " + feedPid);
    }

    protected void updateDataCloudBuildNumber() {
        String currentDataCloudBuildNumber = columnMetadataProxy.latestBuildNumber();
        DataCollection.Version initialVersion = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
        DataCollectionStatus status = dataCollectionProxy.getOrCreateDataCollectionStatus(mainTestTenant.getId(),
                initialVersion);
        status.setDataCloudBuildNumber(currentDataCloudBuildNumber);
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(mainTestTenant.getId(), status, initialVersion);
        log.info(String.format("Update datacloud rebuild number as %s for tenant %s with version %s",
                currentDataCloudBuildNumber, mainTestTenant.getId(), initialVersion));
    }

    private void copyEntitySeedTables(String checkpoint, String checkpointVersion) {
        if (!isEntityMatchEnabled()) {
            log.info("Tenant {} is not an entity match tenant, skip copying entity seed/lookup tables",
                    mainTestTenant.getId());
            return;
        }
        ThreadPoolUtils.doInParallel(Arrays.asList( //
                BusinessEntity.Account.name(), //
                BusinessEntity.Contact.name() //
        ), entity -> copyEntitySeedTable(checkpoint, checkpointVersion, entity));
    }

    private void copyEntitySeedTable(String checkpoint, String checkpointVersion, String entity) {
        EntityPublishRequest request = new EntityPublishRequest();
        request.setEntity(entity);
        String srcTenantId = getCheckPointTenantId(checkpoint, checkpointVersion, entity);
        Tenant srcTenant = new Tenant(CustomerSpace.parse(srcTenantId).toString());
        request.setSrcTenant(srcTenant);
        request.setDestTenant(mainTestTenant);
        request.setDestEnv(EntityMatchEnvironment.SERVING);
        request.setDestTTLEnabled(true);
        request.setBumpupVersion(false);
        log.info("Start copying entity match table for " + entity + " using request: " + JsonUtils.serialize(request));
        EntityPublishStatistics stats = matchProxy.publishEntity(request);
        log.info("Copied {} {} seeds and {} {} lookup entries from tenant {} to tenant {}", stats.getSeedCount(),
                entity, //
                stats.getLookupCount(), entity, //
                CustomerSpace.shortenCustomerSpace(srcTenant.getId()), //
                CustomerSpace.shortenCustomerSpace(mainTestTenant.getId()));
        Assert.assertTrue(stats.getSeedCount() > 0);
        Assert.assertTrue(stats.getLookupCount() > 0);
    }

    private String parseSystemInfos(String checkpoint, String customerSpace) throws IOException {
        updateImportSystemInfos(checkpoint, customerSpace);
        Map<String, DataFeedTask> dataFeedTaskMap = uploadDataFeedTasks(checkpoint, customerSpace);
        if (MapUtils.isEmpty(dataFeedTaskMap)) {
            return null;
        }
        Map<String,AtlasStream> atlasStreamMap = uploadAtlasStream(checkpoint, customerSpace, dataFeedTaskMap);
        Map<String, Catalog> catalogMap = uploadCatalog(checkpoint, customerSpace, dataFeedTaskMap);
        uploadStreamDimension(checkpoint, atlasStreamMap, catalogMap);
        uploadActivityMetricGroup(checkpoint, atlasStreamMap);
        return uploadDimensionMetadata(checkpoint, customerSpace, atlasStreamMap);
    }

    private void updateImportSystemInfos(String checkpoint, String customerSpace)
            throws IOException {
        String jsonFile = String.format("%s/%s/S3ImportSystems.json", checkpointDir, checkpoint);
        if (!new File(jsonFile).exists()) {
            log.error("Can't find S3ImportSystemInfos.");
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
            log.error("S3ImportSystem List is empty.");
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
        String jsonFile = String.format("checkpoints/%s/DataFeed/templateTables.json", checkpoint);
        if (!new File(jsonFile).exists()) {
            log.error("Can't find TemplateInfos.");
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
            log.error("TemplateTables is empty.");
        }
        return templateTableMaps;
    }

    private Map<String, DataFeedTask> uploadDataFeedTasks(String checkpoint, String customerSpace) throws IOException {
        String jsonFile = String.format("checkpoints/%s/DataFeed/DataFeedTasks.json", checkpoint);
        if (!new File(jsonFile).exists()) {
            log.error("Can't find DataFeedTaskInfos.");
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
            for (DataFeedTask dataFeedTask : dataFeedTaskList) {
                DataFeedTask newDataFeedTask = new DataFeedTask();
                String templateName = NamingUtils.timestamp(dataFeedTask.getEntity());
                if (templateTableMaps.containsKey(dataFeedTask.getFeedType())) {
                    Table templateTable = templateTableMaps.get(dataFeedTask.getFeedType());
                    templateTable.setName(templateName);
                    metadataProxy.createTable(customerSpace, templateName, templateTable);
                    newDataFeedTask.setImportTemplate(templateTable);
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
            log.error("DataFeedTaskLists is empty");
        }
        return dataFeedTaskUniqueIdMaps;
    }

    private Map<String, AtlasStream> uploadAtlasStream(String checkpoint, String customerSpace,
                                              Map<String, DataFeedTask> dataFeedTaskIdMaps) throws IOException {
        String jsonFile = String.format("checkpoints/%s/AtlasData/AtlasStreams.json", checkpoint);
        if (!new File(jsonFile).exists()) {
            log.error("Can't find AtlasStreamInfos.");
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
            }

        } else {
            log.error("atlasStreamList is empty.");
        }
        return atlasStreamIdMaps;
    }

    private Map<String, Catalog> uploadCatalog(String checkpoint,String customerSpace,
                                               Map<String, DataFeedTask> dataFeedTaskIdMaps) throws IOException {
        String jsonFile = String.format("checkpoints/%s/AtlasData/Catalogs.json", checkpoint);
        if (!new File(jsonFile).exists()) {
            log.error("Can't find catalogs.");
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
            log.error("catalog list is empty.");
        }
        return catalogMap;
    }

    private void uploadStreamDimension(String checkpoint, Map<String, AtlasStream> atlasStreamIdMaps, Map<String,
            Catalog> catalogMap) throws IOException {
        String jsonFile = String.format("checkpoints/%s/AtlasData/StreamDimensions.json", checkpoint);
        if (!new File(jsonFile).exists()) {
            log.error("Can't find StreamDimensions.");
            return;
        }
        Map<?, ?> dimensionMapObjects = JsonUtils.deserialize(new FileInputStream(jsonFile), Map.class);
        Map<String, StreamDimension> dimensionMap = JsonUtils.convertMap(dimensionMapObjects, String.class,
                StreamDimension.class);
        if (dimensionMap != null && !dimensionMap.isEmpty()) {
            for (Map.Entry<String, StreamDimension> entry : dimensionMap.entrySet()) {
                StreamDimension streamDimension = entry.getValue();
                String oldStreamId = entry.getKey();
                StreamDimension newDimension = new StreamDimension();
                newDimension.setCalculator(streamDimension.getCalculator());
                newDimension.setName(streamDimension.getName());
                newDimension.setDisplayName(streamDimension.getDisplayName());
                newDimension.setGenerator(streamDimension.getGenerator());
                newDimension.setStream(atlasStreamIdMaps.get(oldStreamId));
                Catalog oldCatalog = streamDimension.getCatalog();
                newDimension.setCatalog(catalogMap.get(oldCatalog.getCatalogId()));
                newDimension.setUsages(streamDimension.getUsages());
                streamDimensionEntityMgr.create(newDimension);
            }
        } else {
            log.error("StreamDimension list is empty.");
        }
    }

    private void uploadActivityMetricGroup(String checkpoint, Map<String, AtlasStream> atlasStreamIdMaps) throws IOException {
        String jsonFile = String.format("checkpoints/%s/AtlasData/ActivityMetricGroups.json", checkpoint);
        if (!new File(jsonFile).exists()) {
            log.error("Can't find ActivityMetricGroups.");
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
                        newMetricGroup.setSubCategoryTmpl(activityMetricsGroup.getSubCategoryTmpl());
                        newMetricGroup.setDescriptionTmpl(activityMetricsGroup.getDescriptionTmpl());
                        newMetricGroup.setDisplayNameTmpl(activityMetricsGroup.getDisplayNameTmpl());
                        newMetricGroup.setRollupDimensions(activityMetricsGroup.getRollupDimensions());
                        newMetricGroup.setAggregation(activityMetricsGroup.getAggregation());
                        newMetricGroup.setEntity(activityMetricsGroup.getEntity());
                        newMetricGroup.setSubCategoryTmpl(activityMetricsGroup.getSecondarySubCategoryTmpl());
                        newMetricGroup.setJavaClass(activityMetricsGroup.getJavaClass());
                        newMetricGroup.setNullImputation(activityMetricsGroup.getNullImputation());
                        newMetricGroup.setReducer(activityMetricsGroup.getReducer());
                        activityMetricsGroupEntityMgr.create(newMetricGroup);
                    }
                }
             }
        } else {
            log.error("ActivityMetricGroup list is empty.");
        }
    }

    private String uploadDimensionMetadata(String checkpoint,
                                         String customerSpace, Map<String, AtlasStream> atlasStreamMap) throws IOException {
        String jsonFile = String.format("checkpoints/%s/AtlasData/DimensionMetadatas.json", checkpoint);
        if (!new File(jsonFile).exists()) {
            log.error("Can't find DimensionMetadatas.");
            return null;
        }
        Map<?, Map> dimensionMetadataMapObjects = JsonUtils.deserialize(new FileInputStream(jsonFile), Map.class);
        Map<String, Map> dimensionMetadataMap =
                JsonUtils.convertMap(dimensionMetadataMapObjects, String.class,
                Map.class);
        Map<String, Map<String, DimensionMetadata>> newDimensionMetadaMap = new HashMap<>();
        if (MapUtils.isNotEmpty(dimensionMetadataMap)) {
            for (Map.Entry<String, Map> entry : dimensionMetadataMap.entrySet()) {
                String oldStreamId = entry.getKey();
                AtlasStream atlasStream = atlasStreamMap.get(oldStreamId);
                Map<String, DimensionMetadata> metadataMap = JsonUtils.convertMap(entry.getValue(), String.class,
                        DimensionMetadata.class);
                newDimensionMetadaMap.put(atlasStream.getStreamId(), metadataMap);
            }
            return activityStoreService.saveDimensionMetadata(customerSpace, null, newDimensionMetadaMap);
        } else {
            log.error("dimensionMetadata is empty.");
            return null;
        }
    }

    private String getGroupId(String groupName) {
        String base = ActivityMetricsGroupUtils.fromGroupNameToGroupIdBase(groupName);
        return activityMetricsGroupEntityMgr.getNextAvailableGroupId(base);
    }
}
