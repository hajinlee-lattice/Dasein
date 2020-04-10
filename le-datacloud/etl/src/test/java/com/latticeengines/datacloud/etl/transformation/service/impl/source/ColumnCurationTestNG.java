package com.latticeengines.datacloud.etl.transformation.service.impl.source;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.source.ColumnCurationTransformer;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn.Calculation;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.source.ColumnCurationConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class ColumnCurationTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ColumnCurationTestNG.class);

    private GeneralSource baseSource = new GeneralSource("DnBCacheSeedRaw");

    String targetSourceName = "DnBCacheSeedProcessed";

    @Inject
    protected SourceColumnEntityMgr sourceColumnEntityMgr;

    @Test(groups = "pipeline1")
    public void testTransformation() {
        prepareDnBCacheSeed(false);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return targetSourceName;
    }

    private String getColumnOperationConfig() {
        ColumnCurationConfig conf = new ColumnCurationConfig();
        Calculation[] calArray = { Calculation.MOCK_UP, Calculation.DEPRECATED };
        conf.setColumnOperations(calArray);
        return JsonUtils.serialize(conf);
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("ColumnBatchProcess");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<String>();
        baseSources.add(baseSource.getSourceName());
        step1.setBaseSources(baseSources);
        step1.setTransformer(ColumnCurationTransformer.TRANSFORMER_NAME);
        step1.setTargetSource(targetSourceName);
        String confStr = getColumnOperationConfig();
        step1.setConfiguration(confStr);

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step1);

        // -----------
        configuration.setSteps(steps);

        return configuration;
    }

    // ID, SALES_VOLUME_US_DOLLARS, SALES_VOLUME_RELIABILITY_CODE,
    // EMPLOYEES_TOTAL, EMPLOYEES_TOTAL_RELIABILITY_CODE,
    // EMPLOYEES_HERE, EMPLOYEES_HERE_RELIABILITY_CODE
    private Object[][] data = new Object[][] { //
            { 1, 0L, null, 1, null, 1, null }, //
            { 2, 0L, "2", 1, null, 1, null }, //
            { 3, 1L, null, 0, null, 1, null }, //
            { 4, 1L, null, 0, "2", 1, null }, //
            { 5, 1L, null, 1, null, 0, null }, //
            { 6, 1L, null, 1, null, 0, "2" }, //
            { 7, 0L, null, 0, null, 0, null }, //
            { 8, 0L, "1", 0, "1", 0, "1" }, //
            { 9, null, null, null, null, null, null }, //
    };

    private void prepareDnBCacheSeed(boolean realData) {
        if (realData == true) {
            uploadBaseAvro(baseSource, baseSourceVersion);
        } else {
            List<Pair<String, Class<?>>> columns = new ArrayList<>();
            columns.add(Pair.of("ID", Integer.class));
            columns.add(Pair.of("SALES_VOLUME_US_DOLLARS", Long.class));
            columns.add(Pair.of("SALES_VOLUME_RELIABILITY_CODE", String.class));
            columns.add(Pair.of("EMPLOYEES_TOTAL", Integer.class));
            columns.add(Pair.of("EMPLOYEES_TOTAL_RELIABILITY_CODE", String.class));
            columns.add(Pair.of("EMPLOYEES_HERE", Integer.class));
            columns.add(Pair.of("EMPLOYEES_HERE_RELIABILITY_CODE", String.class));
            uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, columns, data);
        }
    }

    @Override
    protected void uploadBaseAvro(Source baseSource, String baseSourceVersion) {
        InputStream baseAvroStream = ClassLoader.getSystemResourceAsStream("sources/prime_10k.avro");
        String targetPath = hdfsPathBuilder.constructSnapshotDir(baseSource.getSourceName(), baseSourceVersion)
                .append("part-0000.avro").toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, targetPath)) {
                HdfsUtils.rmdir(yarnConfiguration, targetPath);
            }
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, baseAvroStream, targetPath);
            InputStream stream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
            String successPath = hdfsPathBuilder.constructRawDir(baseSource).append(baseSourceVersion)
                    .append("_SUCCESS").toString();
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, successPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        hdfsSourceEntityMgr.setCurrentVersion(baseSource, baseSourceVersion);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");

        // Read SourceColumn table to find the columns to mock up and deprecate
        Map<String, String> fieldValueMap = new HashMap<>();
        Set<String> depSet = new HashSet<>();
        for (SourceColumn column : sourceColumnEntityMgr.getSourceColumns(baseSource.getSourceName())) {
            if (column.getCalculation() == Calculation.MOCK_UP) {
                fieldValueMap.put(column.getColumnName(),
                        StringUtils.isBlank(column.getArguments()) ? null : column.getArguments());
            }
            if (column.getCalculation() == Calculation.DEPRECATED) {
                depSet.add(column.getColumnName());
            }
        }
        log.info("Columns that needs to remove are: " + depSet.toString());
        log.info("Columns that needs to insert are: " + fieldValueMap.keySet().toString());

        while (records.hasNext()) {
            GenericRecord record = records.next();
            for (String depField : depSet) {
                Assert.assertEquals(record.get(depField), null);
            }
            for (String field : fieldValueMap.keySet()) {
                if (fieldValueMap.get(field) == null) {
                    Assert.assertEquals(record.get(field), null);
                } else {
                    Assert.assertEquals(record.get(field).toString(), fieldValueMap.get(field));
                }
            }
        }
    }
}
