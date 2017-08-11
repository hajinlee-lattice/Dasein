package com.latticeengines.apps.cdl.end2end.dataingestion;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
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
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;

@Component("checkpointService")
public class CheckpointService {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointService.class);

    public static final int ACCOUNT_IMPORT_SIZE_1 = 200;
    public static final int ACCOUNT_IMPORT_SIZE_2 = 100;

    public static final int CONTACT_IMPORT_SIZE_1 = 200;
    public static final int CONTACT_IMPORT_SIZE_2 = 100;

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

        Assert.assertEquals(countTableRole(TableRoleInCollection.ConsolidatedAccount), ACCOUNT_IMPORT_SIZE_1);
        Assert.assertEquals(countTableRole(TableRoleInCollection.ConsolidatedContact), CONTACT_IMPORT_SIZE_1);
    }

    public void verifySecondConsolidateCheckpoint() throws IOException {
        verifyHdfsCheckpoint();
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainTestTenant.getId());
        Assert.assertEquals(DataFeed.Status.Active, dataFeed.getStatus());

        verifyStatistics();

        Assert.assertEquals(countTableRole(TableRoleInCollection.ConsolidatedAccount), ACCOUNT_IMPORT_SIZE_1 + ACCOUNT_IMPORT_SIZE_2);
        Assert.assertEquals(countTableRole(TableRoleInCollection.ConsolidatedContact), CONTACT_IMPORT_SIZE_1 + CONTACT_IMPORT_SIZE_2);
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

    long countTableRole(TableRoleInCollection role) {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        Table table = dataCollectionProxy.getTable(customerSpace.toString(), role);
        if (table == null) {
            Assert.fail("Cannot find table in role " + role);
        }
        return table.getExtracts().get(0).getProcessedRecords();
    }

    long countInRedshift(BusinessEntity entity) {
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

    public void cleanup() {
        FileUtils.deleteQuietly(new File(checkpointDir));
    }

}
