package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.SourceCopier;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;

/**
 * This is a deployment test, as it needs metadata ms
 */
public class PipelineFrameworkDeploymentTestNG extends PipelineTransformationDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PipelineFrameworkDeploymentTestNG.class);

    private static final String baseSourceName = "Source1";
    private static final String targetSourceName = "Source2";
    private static final String avroFile = "HGData";
    private static final String intermediateTable =  "IntermediateTable";

    private GeneralSource source1 = new GeneralSource(baseSourceName);

    private ThreadLocal<PipelineTransformationConfiguration> currentConfig = new ThreadLocal<>();
    private ThreadLocal<TableSource> targetTableSource = new ThreadLocal<>();
    private ThreadLocal<Integer> expectedCount = new ThreadLocal<>();

    @BeforeMethod(groups = "deployment")
    public void beforeMethod() {
        prepareCleanPod(source.getSourceName());
        expectedCount.set(1269);
    }

    @AfterMethod(groups = "deployment")
    public void afterMethod() {
        cleanupProgressTables();
        cleanupRegisteredTable();
    }

    @Test(groups = "deployment", enabled = true)
    public void testSourceToSource() {
        targetVersion = HdfsPathBuilder.dateFormat.format(new Date());
        uploadBaseSourceFile(source1, avroFile, baseSourceVersion);
        currentConfig.set(sourceToSource());
        targetTableSource.set(null);

        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
    }

    @Test(groups = "deployment", enabled = true)
    public void testSourceToTable() {
        targetVersion = HdfsPathBuilder.dateFormat.format(new Date());
        uploadBaseSourceFile(source1, avroFile, baseSourceVersion);
        currentConfig.set(sourceToTable());

        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);

        targetTableSource.set(convertTargetTableSource());
        confirmResultFile(progress);
        verifyRegisteredTable();
    }

    @Test(groups = "deployment", enabled = true)
    public void testTableToSource() {
        targetVersion = HdfsPathBuilder.dateFormat.format(new Date());
        uploadAndRegisterTableSource(avroFile, source1.getSourceName());
        currentConfig.set(tableToSource());
        targetTableSource.set(null);

        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
    }

    @Test(groups = "deployment", enabled = true)
    public void testTableToTable() {
        targetVersion = HdfsPathBuilder.dateFormat.format(new Date());
        uploadAndRegisterTableSource(avroFile, source1.getSourceName());
        currentConfig.set(tableToTable());

        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);

        targetTableSource.set(convertTargetTableSource());
        expectedCount.set(2538);
        confirmResultFile(progress);
        verifyRegisteredTable();

        // cleanup intermediate table
        String intermediateTableFullName = TableSource.getFullTableName(intermediateTable, targetVersion);
        metadataProxy.deleteTable(DataCloudConstants.SERVICE_CUSTOMERSPACE, intermediateTableFullName);
    }

    @Override
    protected String getTargetSourceName() {
        return targetSourceName;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(source1.getSourceName(), baseSourceVersion).toString();
    }

    @Override
    protected TableSource getTargetTableSource() {
        return targetTableSource.get();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        return currentConfig.get();
    }

    private TableSource convertTargetTableSource() {
        return hdfsSourceEntityMgr.materializeTableSource(targetSourceName + "_" + targetVersion,
                CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE));
    }

    private PipelineTransformationConfiguration sourceToSource() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("PplFmwkTest");
            configuration.setVersion(targetVersion);
            // -----------
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = Collections.singletonList(source1.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(SourceCopier.TRANSFORMER_NAME);
            step1.setTargetSource(targetSourceName);
            step1.setConfiguration("{}");
            // -----------
            List<TransformationStepConfig> steps = Collections.singletonList(step1);
            // -----------
            configuration.setSteps(steps);
            configuration.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
            return configuration;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private PipelineTransformationConfiguration sourceToTable() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("PplFmwkTest");
            configuration.setVersion(targetVersion);
            // -----------
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = Collections.singletonList(source1.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(SourceCopier.TRANSFORMER_NAME);
            TargetTable targetTable = new TargetTable();
            targetTable.setCustomerSpace(CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE));
            targetTable.setNamePrefix(targetSourceName);
            step1.setTargetTable(targetTable);
            step1.setConfiguration("{}");
            // -----------
            List<TransformationStepConfig> steps = Collections.singletonList(step1);
            // -----------
            configuration.setSteps(steps);
            configuration.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
            return configuration;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private PipelineTransformationConfiguration tableToSource() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("PplFmwkTest");
            configuration.setVersion(targetVersion);
            // -----------
            // step 1 uses external table as base source
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = Collections.singletonList(source1.getSourceName());
            step1.setBaseSources(baseSources);

            SourceTable sourceTable = new SourceTable(source1.getSourceName(), CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE));
            Map<String, SourceTable> baseTables = new HashMap<>();
            baseTables.put(source1.getSourceName(), sourceTable);
            step1.setBaseTables(baseTables);

            step1.setTransformer(SourceCopier.TRANSFORMER_NAME);
            step1.setTargetSource(targetSourceName);
            step1.setConfiguration("{}");
            // -----------
            List<TransformationStepConfig> steps = Collections.singletonList(step1);
            // -----------
            configuration.setSteps(steps);
            configuration.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
            return configuration;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private PipelineTransformationConfiguration tableToTable() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("PplFmwkTest");
            configuration.setVersion(targetVersion);
            // -----------
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = Collections.singletonList(source1.getSourceName());
            step1.setBaseSources(baseSources);

            SourceTable sourceTable = new SourceTable(source1.getSourceName(), CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE));
            Map<String, SourceTable> baseTables = new HashMap<>();
            baseTables.put(source1.getSourceName(), sourceTable);
            step1.setBaseTables(baseTables);

            step1.setTransformer(SourceCopier.TRANSFORMER_NAME);
            TargetTable targetTable = new TargetTable();
            targetTable.setCustomerSpace(CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE));
            targetTable.setNamePrefix(intermediateTable);
            step1.setTargetTable(targetTable);
            step1.setConfiguration("{}");
            // -----------
            // step 2 uses original table and step 1 output as table input
            TransformationStepConfig step2 = new TransformationStepConfig();

            // original table
            step2.setBaseSources(baseSources);
            step2.setBaseTables(baseTables);

            // step 1 output
            step2.setInputSteps(Collections.singletonList(0));
            step2.setTransformer(SourceCopier.TRANSFORMER_NAME);

            // output is also a table
            TargetTable targetTable2 = new TargetTable();
            targetTable2.setCustomerSpace(CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE));
            targetTable2.setNamePrefix(targetSourceName);
            step2.setTargetTable(targetTable2);
            step2.setConfiguration("{}");
            // -----------
            List<TransformationStepConfig> steps = Arrays.asList(step1, step2);
            // -----------
            configuration.setSteps(steps);
            return configuration;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void verifyRegisteredTable() {
        TableSource tableSource = getTargetTableSource();
        Table table = metadataProxy.getTable(tableSource.getCustomerSpace().toString(), tableSource.getTable().getName());
        Assert.assertNotNull(table);
        List<Attribute> attributes = table.getAttributes();
        Assert.assertEquals(attributes.size(), 12); // 12 attributes
    }

    private void cleanupRegisteredTable() {
        TableSource tableSource = getTargetTableSource();
        if (tableSource != null) {
            metadataProxy.deleteTable(tableSource.getCustomerSpace().toString(), tableSource.getTable().getName());
        }
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        // correctness is tested in the dataflow functional test
        log.info("Start to verify records one by one.");
        Integer rowCount = 0;
        while (records.hasNext()) {
            @SuppressWarnings("unused")
            GenericRecord record = records.next();
//            if (rowCount < 10) {
//                System.out.println(record);
//            }
            rowCount++;
        }
        Assert.assertEquals(rowCount, expectedCount.get());
    }

}
