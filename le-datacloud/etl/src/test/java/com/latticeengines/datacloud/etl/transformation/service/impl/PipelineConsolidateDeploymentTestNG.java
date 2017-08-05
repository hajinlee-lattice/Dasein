package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDeltaTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

public class PipelineConsolidateDeploymentTestNG extends PipelineTransformationDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PipelineConsolidateDeploymentTestNG.class);

    private String tableName1 = "ConsolidateTable1";
    private String tableName2 = "ConsolidateTable2";
    private String masterTableName = "MasterTable";
    private static final String mergedTableName = "MergedTable";
    private static final String deltaTableName = "DeltaTable";

    private static final CustomerSpace customerSpace = CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE);

    private PipelineTransformationConfiguration currentConfig = null;
    private TableSource targetTableSource = null;

    @Value("${datacloud.match.default.decision.graph}")
    private String defaultGraph;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @BeforeMethod(groups = "deployment")
    public void beforeMethod() {
        prepareCleanPod("PipelineConsolidateDeploymentTestNG");
    }

    @AfterMethod(groups = "deployment")
    public void afterMethod() {

        cleanupProgressTables();

        // cleanup intermediate table
        cleanupRegisteredTable(tableName1);
        cleanupRegisteredTable(tableName2);
        cleanupRegisteredTable(TableSource.getFullTableName(mergedTableName, targetVersion));
        cleanupRegisteredTable(TableSource.getFullTableName(masterTableName, targetVersion));
        cleanupRegisteredTable(TableSource.getFullTableName(deltaTableName, targetVersion));

        prepareCleanPod("PipelineConsolidateDeploymentTestNG");
    }

    @Test(groups = "deployment", enabled = true)
    public void testTableToTable() {
        targetVersion = HdfsPathBuilder.dateFormat.format(new Date());
        uploadAndRegisterTableSource(tableName1, tableName1, "ID", null);
        uploadAndRegisterTableSource(tableName2, tableName2, "ID", null);
        uploadAndRegisterTableSource(masterTableName, masterTableName, "ID", null);
        currentConfig = getConcolidateConfig();

        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);

        verifyMergedTable();

        verifyDeltaTable();
        confirmResultFile(progress);

    }

    private void verifyDeltaTable() {
        targetTableSource = convertTargetTableSource(deltaTableName);
        String deltaTableFullName = TableSource.getFullTableName(deltaTableName, targetVersion);
        verifyRegisteredTable(deltaTableFullName, 6);
    }

    private void verifyMergedTable() {
        String mergedTableFullName = TableSource.getFullTableName(mergedTableName, targetVersion);
        verifyRegisteredTable(mergedTableFullName, 4);
        verifyRecordsInMergedTable(mergedTableFullName);
    }

    private void verifyRecordsInMergedTable(String mergedTableFullName) {
        List<GenericRecord> records = getRecordFromTable(mergedTableFullName);
        log.info("Start to verify records one by one.");
        Integer rowCount = 0;
        Map<String, GenericRecord> recordMap = new HashMap<>();
        for (GenericRecord record : records) {
            String id = String.valueOf(record.get("ID"));
            recordMap.put(id, record);
            rowCount++;
        }
        Assert.assertEquals(rowCount, new Integer(5));
        GenericRecord record = recordMap.get("1");
        Assert.assertEquals(record.get("Domain").toString(), "google.com");
        Assert.assertEquals(record.get("Email").toString(), "123@google.com");
        Assert.assertEquals(record.get("FirstName").toString(), "John");

        record = recordMap.get("2");
        Assert.assertEquals(record.get("Domain").toString(), "oracle.com");
        Assert.assertEquals(record.get("Email").toString(), "234@oracle.com");
        Assert.assertEquals(record.get("FirstName").toString(), "Smith");

        record = recordMap.get("3");
        Assert.assertEquals(record.get("Domain").toString(), "salesforce.com");
        Assert.assertEquals(record.get("Email"), null);
        Assert.assertEquals(record.get("FirstName"), null);

        record = recordMap.get("4");
        Assert.assertEquals(record.get("Domain").toString(), "microsoft.com");
        Assert.assertEquals(record.get("Email").toString(), "234@d.com");
        Assert.assertEquals(record.get("FirstName").toString(), "Marry");

        record = recordMap.get("5");
        Assert.assertEquals(record.get("Domain").toString(), "faceboook.com");
        Assert.assertEquals(record.get("Email"), null);
        Assert.assertEquals(record.get("FirstName"), null);
    }

    @Override
    protected String getTargetSourceName() {
        return masterTableName;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return null;
    }

    @Override
    protected TableSource getTargetTableSource() {
        return targetTableSource;
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        return currentConfig;
    }

    private TableSource convertTargetTableSource(String tableName) {
        return hdfsSourceEntityMgr.materializeTableSource((tableName + "_" + targetVersion), customerSpace);
    }

    private PipelineTransformationConfiguration getConcolidateConfig() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("ConsolidatePipeline");
            configuration.setVersion(targetVersion);

            /* Step 1: Merge */
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = Arrays.asList(tableName1, tableName2);
            step1.setBaseSources(baseSources);

            SourceTable sourceTable1 = new SourceTable(tableName1, customerSpace);
            SourceTable sourceTable2 = new SourceTable(tableName2, customerSpace);
            Map<String, SourceTable> baseTables = new HashMap<>();
            baseTables.put(tableName1, sourceTable1);
            baseTables.put(tableName2, sourceTable2);
            step1.setBaseTables(baseTables);
            step1.setTransformer("consolidateDataTransformer");

            TargetTable targetTable = new TargetTable();
            targetTable.setCustomerSpace(customerSpace);
            targetTable.setNamePrefix(mergedTableName);
            step1.setTargetTable(targetTable);
            step1.setConfiguration(getConsolidateDataConfig());

            /* Step 2: Match */
            TransformationStepConfig step2 = new TransformationStepConfig();
            // step 1 output
            step2.setInputSteps(Collections.singletonList(0));
            step2.setTransformer("bulkMatchTransformer");
            step2.setConfiguration(getMatchConfig());

            /* Step 3: Upsert to Master table */
            TransformationStepConfig step3 = new TransformationStepConfig();
            Table masterTable = metadataProxy.getTable(customerSpace.toString(), masterTableName);
            if (masterTable != null) {
                baseSources = Arrays.asList(masterTableName);
                baseTables = new HashMap<>();
                SourceTable sourceMasterTable = new SourceTable(masterTableName, customerSpace);
                baseTables.put(masterTableName, sourceMasterTable);
                step3.setBaseSources(baseSources);
                step3.setBaseTables(baseTables);
            }
            // step 2 output
            step3.setInputSteps(Collections.singletonList(1));
            step3.setTransformer("consolidateDataTransformer");
            step3.setConfiguration(getConsolidateDataConfig());

            targetTable = new TargetTable();
            targetTable.setCustomerSpace(customerSpace);
            targetTable.setNamePrefix(masterTableName);
            step3.setTargetTable(targetTable);

            /* Step 4: Leftjoin for Delta */
            TransformationStepConfig step4 = new TransformationStepConfig();
            // step 2, 3 output
            step4.setInputSteps(Arrays.asList(1, 2));
            step4.setTransformer("consolidateDeltaTransformer");
            step4.setConfiguration(getConsolidateDeltaConfig());

            targetTable = new TargetTable();
            targetTable.setCustomerSpace(customerSpace);
            targetTable.setNamePrefix(deltaTableName);
            step4.setTargetTable(targetTable);

            /* Final */
            List<TransformationStepConfig> steps = Arrays.asList(step1, step2, step3, step4);
            configuration.setSteps(steps);

            return configuration;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String getConsolidateDataConfig() {
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField("ID");
        return JsonUtils.serialize(config);
    }

    private String getConsolidateDeltaConfig() {
        ConsolidateDeltaTransformerConfig config = new ConsolidateDeltaTransformerConfig();
        config.setSrcIdField("ID");
        return JsonUtils.serialize(config);
    }

    private String getMatchConfig() {
        MatchTransformerConfig config = new MatchTransformerConfig();
        MatchInput matchInput = new MatchInput();
        matchInput.setRootOperationUid(UUID.randomUUID().toString().toUpperCase());
        matchInput.setTenant(new Tenant(customerSpace.getTenantId()));
        matchInput.setPredefinedSelection(Predefined.ID);
        matchInput.setKeyMap(getKeyMap());
        matchInput.setDecisionGraph("DragonClaw");
        matchInput.setExcludePublicDomain(false);
        matchInput.setPublicDomainAsNormalDomain(true);
        matchInput.setDataCloudVersion(getDataCloudVersion());
        matchInput.setSkipKeyResolution(true);
        matchInput.setUseDnBCache(true);
        matchInput.setUseRemoteDnB(false);
        matchInput.setLogDnBBulkResult(false);
        matchInput.setMatchDebugEnabled(false);

        config.setMatchInput(matchInput);
        return JsonUtils.serialize(config);
    }

    private Map<MatchKey, List<String>> getKeyMap() {
        Map<MatchKey, List<String>> keyMap = new TreeMap<>();
        keyMap.put(MatchKey.Domain, Arrays.asList("Domain"));
        // keyMap.put(MatchKey.Name, Arrays.asList("Name"));
        // keyMap.put(MatchKey.Country, Arrays.asList("Country"));
        // keyMap.put(MatchKey.State, Arrays.asList("State"));
        // keyMap.put(MatchKey.City, Arrays.asList("City"));
        // // keyMap.put(MatchKey.Zipcode, Arrays.asList("Zipcode"));
        // keyMap.put(MatchKey.PhoneNumber, Arrays.asList("PhoneNumber"));
        return keyMap;
    }

    private String getDataCloudVersion() {
        return columnMetadataProxy.latestVersion(null).getVersion();
    }

    private void verifyRegisteredTable(String tableName, int attrs) {
        Table table = metadataProxy.getTable(customerSpace.toString(), tableName);
        Assert.assertNotNull(table);
        List<Attribute> attributes = table.getAttributes();
        Assert.assertEquals(new Integer(attributes.size()), new Integer(attrs));
    }

    private void cleanupRegisteredTable(String tableName) {
        metadataProxy.deleteTable(customerSpace.toString(), tableName);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {

        log.info("Start to verify records one by one.");
        Integer rowCount = 0;
        Map<String, GenericRecord> recordMap = new HashMap<>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String id = String.valueOf(record.get("ID"));
            recordMap.put(id, record);
            rowCount++;
        }
        Assert.assertEquals(rowCount, new Integer(5));
        GenericRecord record = recordMap.get("1");
        Assert.assertEquals(record.get("Domain").toString(), "google.com");
        Assert.assertEquals(record.get("Email").toString(), "123@google.com");
        Assert.assertEquals(record.get("FirstName").toString(), "John");
        Assert.assertEquals(record.get("LastName"), null);
        Assert.assertEquals(record.get("LatticeAccountId").toString(), "20015989333");

        record = recordMap.get("2");
        Assert.assertEquals(record.get("Domain").toString(), "oracle.com");
        Assert.assertEquals(record.get("Email").toString(), "234@oracle.com");
        Assert.assertEquals(record.get("FirstName").toString(), "Smith");
        Assert.assertEquals(record.get("LastName").toString(), "last2");
        Assert.assertEquals(record.get("LatticeAccountId").toString(), "120011803715");

        record = recordMap.get("3");
        Assert.assertEquals(record.get("Domain").toString(), "salesforce.com");
        Assert.assertEquals(record.get("Email"), null);
        Assert.assertEquals(record.get("FirstName"), null);
        Assert.assertEquals(record.get("LastName"), null);
        Assert.assertEquals(record.get("LatticeAccountId").toString(), "90007520313");

        record = recordMap.get("4");
        Assert.assertEquals(record.get("Domain").toString(), "microsoft.com");
        Assert.assertEquals(record.get("Email").toString(), "234@d.com");
        Assert.assertEquals(record.get("FirstName").toString(), "Marry");
        Assert.assertEquals(record.get("LastName").toString(), "last4");
        Assert.assertEquals(record.get("LatticeAccountId").toString(), "90006476137");

        record = recordMap.get("5");
        Assert.assertEquals(record.get("Domain").toString(), "faceboook.com");
        Assert.assertEquals(record.get("Email"), null);
        Assert.assertEquals(record.get("FirstName").toString(), "faceboookFirst");
        Assert.assertEquals(record.get("LastName").toString(), "last5");
        Assert.assertEquals(record.get("LatticeAccountId").toString(), "55");

    }

    // @Test(groups = "deployment", enabled = true)
    public void createData() {
        uploadTable1();
        uploadTable2();
        uploadMasterTable();
    }

    private void uploadTable1() {
        Object[][] data = { { 1, "google.com", "123@google.com" }, //
                { 2, "oracle.com", "234@oracle.com" }, //
                { 3, "salesforce.com", null }, //
                { 4, null, "234@d.com" } //
        };

        List<String> fieldNames = Arrays.asList("ID", "Domain", "Email");
        List<Class<?>> clz = Arrays.asList((Class<?>) Integer.class, String.class, String.class);
        uploadDataToHdfs(data, fieldNames, clz, "/" + "PipelineConsolidateDeploymentTestNG" + "/" + tableName1
                + ".avro", tableName1);
    }

    private void uploadTable2() {
        Object[][] data = { { 1, "x.com", "John" }, //
                { 2, "y.com", "Smith" }, //
                { 4, "microsoft.com", "Marry" }, //
                { 5, "faceboook.com", null } //
        };
        List<String> fieldNames = Arrays.asList("ID", "Domain", "FirstName");
        List<Class<?>> clz = Arrays.asList((Class<?>) Integer.class, String.class, String.class);
        uploadDataToHdfs(data, fieldNames, clz, "/" + "PipelineConsolidateDeploymentTestNG" + "/" + tableName2
                + ".avro", tableName2);

    }

    private void uploadMasterTable() {
        Object[][] data = { { 2, "oracledummy.com", "Smithdummy", "last2", "22" }, //
                { 4, "microsoft.com", "Marry", "last4", null }, //
                { 5, null, "faceboookFirst", "last5", "55" }, //
                { 6, "facebookdummy.com", "facebookFirstDummy", "last6", "66" } //
        };
        List<String> fieldNames = Arrays.asList("ID", "Domain", "FirstName", "LastName", "LatticeAccountId");
        List<Class<?>> clz = Arrays.asList((Class<?>) Integer.class, String.class, String.class, String.class,
                String.class);
        uploadDataToHdfs(data, fieldNames, clz, "/" + "PipelineConsolidateDeploymentTestNG" + "/" + masterTableName
                + ".avro", masterTableName);

    }

}
