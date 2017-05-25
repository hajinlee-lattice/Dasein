package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;

public class PipelineConsolidateDeploymentTestNG extends PipelineTransformationDeploymentTestNGBase {

    private static final Log log = LogFactory.getLog(PipelineConsolidateDeploymentTestNG.class);

    private GeneralSource source1 = new GeneralSource("ConsolidateSource1");
    private GeneralSource source2 = new GeneralSource("ConsolidateSource2");
    private static final String targetSourceName = "ConsolidateTarget";

    private ThreadLocal<PipelineTransformationConfiguration> currentConfig = new ThreadLocal<>();
    private ThreadLocal<TableSource> targetTableSource = new ThreadLocal<>();
    private ThreadLocal<Integer> expectedCount = new ThreadLocal<>();

    @BeforeMethod(groups = "deployment")
    public void beforeMethod() {
        prepareCleanPod(source.getSourceName());
    }

    // @AfterMethod(groups = "deployment")
    public void afterMethod() {
        cleanupProgressTables();
        cleanupRegisteredTable();
    }

    @Test(groups = "deployment", enabled = false)
    public void testTableToTable() {
        targetVersion = HdfsPathBuilder.dateFormat.format(new Date());
        uploadAndRegisterTableSource(source1.getSourceName(), source1.getSourceName(), "ID", null);
        uploadAndRegisterTableSource(source2.getSourceName(), source2.getSourceName(), "ID", null);
        currentConfig.set(getConcolidateConfig());

        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);

        targetTableSource.set(convertTargetTableSource());
        expectedCount.set(5);
        confirmResultFile(progress);
        verifyRegisteredTable();

        // cleanup intermediate table
        String intermediateTableFullName = TableSource.getFullTableName(targetSourceName, targetVersion);
        metadataProxy.deleteTable(DataCloudConstants.SERVICE_CUSTOMERSPACE, intermediateTableFullName);
    }

    // @Test(groups = "deployment", enabled = true)
    public void createData() {
        uploadSource1();
        uploadSource2();
    }

    @Override
    protected String getTargetSourceName() {
        return targetSourceName;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return null;
    }

    @Override
    protected TableSource getTargetTableSource() {
        return targetTableSource.get();
    }

    @Override
    PipelineTransformationConfiguration createTransformationConfiguration() {
        return currentConfig.get();
    }

    private TableSource convertTargetTableSource() {
        return hdfsSourceEntityMgr.materializeTableSource(
                (targetSourceName + "_" + targetVersion),
                CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE));
    }

    private PipelineTransformationConfiguration getConcolidateConfig() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("Consolidate");
            configuration.setVersion(targetVersion);
            // -----------
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = Arrays.asList(source1.getSourceName(), source2.getSourceName());
            step1.setBaseSources(baseSources);

            SourceTable sourceTable1 = new SourceTable(source1.getSourceName(),
                    CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE));
            SourceTable sourceTable2 = new SourceTable(source2.getSourceName(),
                    CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE));
            Map<String, SourceTable> baseTables = new HashMap<>();
            baseTables.put(source1.getSourceName(), sourceTable1);
            baseTables.put(source2.getSourceName(), sourceTable2);
            step1.setBaseTables(baseTables);

            step1.setTransformer("consolidateDataTransformer");
            TargetTable targetTable = new TargetTable();
            targetTable.setCustomerSpace(CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE));
            targetTable.setNamePrefix(targetSourceName);
            step1.setTargetTable(targetTable);
            step1.setConfiguration("{}");

            List<TransformationStepConfig> steps = Arrays.asList(step1);
            configuration.setSteps(steps);
            return configuration;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void verifyRegisteredTable() {
        TableSource tableSource = getTargetTableSource();
        Table table = metadataProxy.getTable(tableSource.getCustomerSpace().toString(), tableSource.getTable()
                .getName());
        Assert.assertNotNull(table);
        List<Attribute> attributes = table.getAttributes();
        Assert.assertEquals(attributes.size(), 4);
    }

    private void cleanupRegisteredTable() {
        TableSource tableSource = getTargetTableSource();
        if (tableSource != null) {
            metadataProxy.deleteTable(tableSource.getCustomerSpace().toString(), tableSource.getTable().getName());
        }
    }

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        // correctness is tested in the dataflow functional test
        log.info("Start to verify records one by one.");
        Integer rowCount = 0;
        Map<String, GenericRecord> recordMap = new HashMap<>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String id = String.valueOf(record.get("ID"));
            recordMap.put(id, record);
            rowCount++;
        }
        Assert.assertEquals(rowCount, expectedCount.get());
        GenericRecord record = recordMap.get("1");
        Assert.assertEquals(record.get("Domain").toString(), "a.com");
        Assert.assertEquals(record.get("Email").toString(), "123@a.com");
        Assert.assertEquals(record.get("FirstName").toString(), "John");

        record = recordMap.get("2");
        Assert.assertEquals(record.get("Domain").toString(), "b.com");
        Assert.assertEquals(record.get("Email").toString(), "234@a.com");
        Assert.assertEquals(record.get("FirstName").toString(), "Smith");

        record = recordMap.get("3");
        Assert.assertEquals(record.get("Domain").toString(), "c.com");
        Assert.assertEquals(record.get("Email"), null);
        Assert.assertEquals(record.get("FirstName"), null);

        record = recordMap.get("4");
        Assert.assertEquals(record.get("Domain").toString(), "x.com");
        Assert.assertEquals(record.get("Email").toString(), "234@d.com");
        Assert.assertEquals(record.get("FirstName").toString(), "Mary");

        record = recordMap.get("5");
        Assert.assertEquals(record.get("Domain").toString(), "e.com");
        Assert.assertEquals(record.get("Email"), null);
        Assert.assertEquals(record.get("FirstName"), null);

    }

    private void uploadSource1() {
        Object[][] data = { { 1, "a.com", "123@a.com" }, //
                { 2, "b.com", "234@a.com" }, //
                { 3, "c.com", null }, //
                { 4, null, "234@d.com" } //
        };

        List<String> fieldNames = Arrays.asList("ID", "Domain", "Email");
        List<Class<?>> clz = Arrays.asList((Class<?>) Integer.class, String.class, String.class);
        uploadDataToHdfs(data, fieldNames, clz, "/" + source.getSourceName() + "/" + source1.getSourceName() + ".avro",
                source1.getSourceName());

    }

    private void uploadSource2() {
        Object[][] data = { { 1, "x.com", "John" }, //
                { 2, "y.com", "Smith" }, //
                { 4, "x.com", "Marry" }, //
                { 5, "e.com", null } //
        };

        List<String> fieldNames = Arrays.asList("ID", "Domain", "FirstName");
        List<Class<?>> clz = Arrays.asList((Class<?>) Integer.class, String.class, String.class);
        uploadDataToHdfs(data, fieldNames, clz, "/" + source.getSourceName() + "/" + source2.getSourceName() + ".avro",
                source2.getSourceName());

    }

}
