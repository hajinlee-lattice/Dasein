package com.latticeengines.apps.cdl.end2end.dataingestion;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;
import com.latticeengines.yarn.exposed.service.JobService;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;

@Component("checkpointService")
public class CheckpointService {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointService.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    protected CDLProxy cdlProxy;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private EaiProxy eaiProxy;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private JobService jobService;

    @Value("${camille.zk.pod.id}")
    private String podId;

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    private ObjectMapper om = new ObjectMapper();

    private Tenant mainTestTenant;

    private String checkpointDir;

    public void setMaintestTenant(Tenant mainTestTenant) {
        this.mainTestTenant = mainTestTenant;
    }

    public void verifyFirstProfileCheckpoint() throws IOException {
        verifyHdfsCheckpoint();
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainTestTenant.getId());
        Assert.assertEquals(DataFeed.Status.Active, dataFeed.getStatus());

        verifyStatistics();

        Assert.assertEquals(countTableRole(TableRoleInCollection.ConsolidatedAccount), 300);
        Assert.assertEquals(countTableRole(TableRoleInCollection.ConsolidatedContact), 300);
        Assert.assertEquals(countInRedshift(BusinessEntity.Account), 300);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), 300);
    }

    public void verifySecondConsolidateCheckpoint() throws IOException {
        verifyHdfsCheckpoint();
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainTestTenant.getId());
        Assert.assertEquals(DataFeed.Status.Active, dataFeed.getStatus());

        verifyStatistics();

        Assert.assertEquals(countTableRole(TableRoleInCollection.ConsolidatedAccount), 500);
        Assert.assertEquals(countTableRole(TableRoleInCollection.ConsolidatedContact), 500);
        Assert.assertEquals(countInRedshift(BusinessEntity.Account), 500);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), 500);
    }

    public void resumeCheckpoint(String checkpoint) throws IOException {
        unzipCheckpoint(checkpoint);

        dataFeedProxy.getDataFeed(mainTestTenant.getId());

        List<TableRoleInCollection> tables = Arrays.asList( //
                TableRoleInCollection.ConsolidatedAccount, //
                TableRoleInCollection.ConsolidatedContact, //
                TableRoleInCollection.BucketedAccount, //
                TableRoleInCollection.SortedContact, //
                TableRoleInCollection.Profile);
        for (TableRoleInCollection role : tables) {
            Table table = parseCheckpointTable(checkpoint, role.name());
            metadataProxy.createTable(mainTestTenant.getId(), table.getName(), table);
            dataCollectionProxy.upsertTable(mainTestTenant.getId(), table.getName(), role);
        }

        StatisticsContainer statisticsContainer = parseCheckpointStatistics(checkpoint);
        dataCollectionProxy.upsertStats(mainTestTenant.getId(), statisticsContainer);

        uploadCheckpointHdfs(checkpoint);
        exportCheckpointToRedshift();

        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Active.name());
    }

    private void unzipCheckpoint(String checkpoint) throws IOException {
        checkpointDir = CustomerSpace.parse(mainTestTenant.getId()).getTenantId();
        String resourcePath = String.format("end2end/%s.zip", checkpoint);
        String zipFilePath = Thread.currentThread().getContextClassLoader().getResource(resourcePath).getPath();
        try {
            ZipFile zipFile = new ZipFile(zipFilePath);
            zipFile.extractAll(checkpointDir);
        } catch (ZipException e) {
            throw new IOException("Failed to unzip checkpoint archive " + zipFilePath, e);
        }
        logger.info(String.format("Unzip checkpoint archive %s to local dir %s", zipFilePath, checkpointDir));
    }

    public long countTableRole(TableRoleInCollection role) {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        Table table = dataCollectionProxy.getTable(customerSpace.toString(), role);
        if (table == null) {
            Assert.fail("Cannot find table in role " + role);
        }
        return table.getExtracts().get(0).getProcessedRecords();
    }

    private long countInRedshift(BusinessEntity entity) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        return entityProxy.getCount(mainTestTenant.getId(), entity, frontEndQuery);
    }

    private void verifyStatistics() {
        StatisticsContainer statisticsContainer = dataCollectionProxy.getStats(mainTestTenant.getId());
        Assert.assertNotNull(statisticsContainer);
        Statistics statistics = statisticsContainer.getStatistics();
        statistics.getCategories().values().forEach(catStats -> //
        catStats.getSubcategories().values().forEach(subCatStats -> {
            subCatStats.getAttributes().forEach(this::verifyAttrStats);
        }));
    }

    private void verifyAttrStats(AttributeLookup lookup, AttributeStats attributeStats) {
        Assert.assertNotNull(attributeStats.getNonNullCount());
        Assert.assertTrue(attributeStats.getNonNullCount() >= 0);
        Buckets buckets = attributeStats.getBuckets();
        if (buckets != null) {
            Assert.assertNotNull(buckets.getType());
            Assert.assertFalse(buckets.getBucketList() == null || buckets.getBucketList().isEmpty(),
                    "Bucket list for " + lookup + " is empty.");
            Long sum = buckets.getBucketList().stream().mapToLong(Bucket::getCount).sum();
            Assert.assertEquals(sum, attributeStats.getNonNullCount());
        }
    }

    private void verifyHdfsCheckpoint() throws IOException {
        CustomerSpace cs = CustomerSpace.parse(mainTestTenant.getId());
        Path dataRoot = PathBuilder.buildCustomerSpacePath(podId, cs).append("Data");
        HdfsUtils.fileExists(yarnConfiguration, dataRoot.toString());
        List<String> tables = HdfsUtils.getFilesForDir(yarnConfiguration, dataRoot.append("Tables").toString());
        Assert.assertFalse(tables.isEmpty());
        for (String tableHdfsPath : tables) {
            String tableName = tableHdfsPath.substring(tableHdfsPath.lastIndexOf("/") + 1);
            if (!"VisiDB".equals(tableName)) {
                logger.info("Checking table " + tableName);
                Iterator<GenericRecord> iterator = AvroUtils.iterator(yarnConfiguration, tableHdfsPath + "/*.avro");
                Assert.assertTrue(iterator.hasNext(), "Table at " + tableHdfsPath + " does not have avro record.");
            }
        }
    }

    private void uploadCheckpointHdfs(String checkpoint) throws IOException {
        String localDir = checkpointDir + "/" + checkpoint + "/hdfs/Data";
        CustomerSpace cs = CustomerSpace.parse(mainTestTenant.getId());
        String targetPath = PathBuilder.buildCustomerSpacePath(podId, cs).append("Data").toString();
        HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, localDir, targetPath);
        logger.info("Upload checkpoint to hdfs path " + targetPath);
    }

    private Table parseCheckpointTable(String checkpoint, String tableName) throws IOException {
        String jsonFile = String.format("%s/%s/tables/%s.json", checkpointDir, checkpoint, tableName);
        JsonNode json = om.readTree(new File(jsonFile));
        String hdfsPath = json.get("extracts_directory").asText();
        Pattern pattern = Pattern.compile("/Contracts/(.*)/Tenants/");
        Matcher matcher = pattern.matcher(hdfsPath);
        String tenantName;
        if (matcher.find()) {
            tenantName = matcher.group(1);
            logger.info("Found tenant name " + tenantName + " in json.");
        } else {
            throw new IOException("Cannot parse a tenant name from json");
        }

        String str = JsonUtils.serialize(json);
        str = str.replaceAll("/Pods/Default/", "/Pods/" + podId + "/");
        str = str.replaceAll(tenantName, CustomerSpace.parse(mainTestTenant.getId()).getTenantId());

        return JsonUtils.deserialize(str, Table.class);
    }

    private StatisticsContainer parseCheckpointStatistics(String checkpoint) throws IOException {
        String statsFile = String.format("%s/%s/stats_container.json", checkpointDir, checkpoint);
        StatisticsContainer statisticsContainer = JsonUtils.deserialize(new FileInputStream(statsFile), StatisticsContainer.class);
        statisticsContainer.setName(NamingUtils.timestamp("Stats"));
        return statisticsContainer;
    }

    private void exportCheckpointToRedshift() {
        logger.info("Exporting checkpoint data to redshift. This may take more than 10 min ...");
        List<TableRoleInCollection> tables = Arrays.asList( //
                TableRoleInCollection.BucketedAccount, //
                TableRoleInCollection.SortedContact);
        for (TableRoleInCollection role : tables) {
            logger.info("Started exporting " + role + " to redshift ...");
            Table table = dataCollectionProxy.getTable(mainTestTenant.getId(), role);
            ExportConfiguration exportConfiguration = setupExportConfig(table, table.getName(), role);
            AppSubmission submission = eaiProxy.submitEaiJob(exportConfiguration);
            int timeout = new Long(TimeUnit.MINUTES.toSeconds(30)).intValue();
            JobStatus completedStatus = jobService.waitFinalJobStatus(submission.getApplicationIds().get(0), timeout);
            Assert.assertEquals(completedStatus.getStatus(), FinalApplicationStatus.SUCCEEDED);
            logger.info("Finished exporting " + role + " to redshift.");
        }
    }

    // Copied from ExportDataToRedshift
    private ExportConfiguration setupExportConfig(Table sourceTable, String targetTableName,
            TableRoleInCollection tableRole) {
        HdfsToRedshiftConfiguration exportConfig = new HdfsToRedshiftConfiguration();
        exportConfig.setExportFormat(ExportFormat.AVRO);
        exportConfig.setCleanupS3(true);
        exportConfig.setCreateNew(false);
        exportConfig.setAppend(false);
        exportConfig.setCustomerSpace(CustomerSpace.parse(mainTestTenant.getId()));
        exportConfig.setExportInputPath(sourceTable.getExtractsDirectory() + "/*.avro");
        exportConfig.setExportTargetPath(sourceTable.getName());
        exportConfig.setNoSplit(true);
        exportConfig.setExportDestination(ExportDestination.REDSHIFT);

        // all distributed on account id
        String distKey = tableRole.getPrimaryKey().name();
        List<String> sortKeys = new ArrayList<>(tableRole.getForeignKeysAsStringList());
        if (!sortKeys.contains(tableRole.getPrimaryKey().name())) {
            sortKeys.add(tableRole.getPrimaryKey().name());
        }
        RedshiftTableConfiguration.SortKeyType sortKeyType = sortKeys.size() == 1
                ? RedshiftTableConfiguration.SortKeyType.Compound : RedshiftTableConfiguration.SortKeyType.Interleaved;

        RedshiftTableConfiguration redshiftTableConfig = new RedshiftTableConfiguration();
        redshiftTableConfig.setS3Bucket(s3Bucket);
        redshiftTableConfig.setDistStyle(RedshiftTableConfiguration.DistStyle.Key);
        redshiftTableConfig.setDistKey(distKey);
        redshiftTableConfig.setSortKeyType(sortKeyType);
        redshiftTableConfig.setSortKeys(sortKeys);
        redshiftTableConfig.setTableName(targetTableName);
        redshiftTableConfig
                .setJsonPathPrefix(String.format("%s/jsonpath/%s.jsonpath", RedshiftUtils.AVRO_STAGE, targetTableName));
        exportConfig.setRedshiftTableConfiguration(redshiftTableConfig);

        return exportConfig;
    }

    public void cleanup() {
        FileUtils.deleteQuietly(new File(checkpointDir));
    }

}
