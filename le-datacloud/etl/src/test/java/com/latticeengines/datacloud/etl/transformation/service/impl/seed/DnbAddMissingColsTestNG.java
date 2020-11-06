package com.latticeengines.datacloud.etl.transformation.service.impl.seed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.seed.DnbMissingColsAddFromPrevFlow;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.seed.DnBAddMissingColsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class DnbAddMissingColsTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DnbAddMissingColsTestNG.class);

    private GeneralSource source = new GeneralSource("DnbSeedWithAddedCols");
    private GeneralSource baseSource1 = new GeneralSource("PrevDnBCacheSeed");
    private GeneralSource baseSource2 = new GeneralSource("NewDnBCacheSeed");

    @Test(groups = "functional")
    public void testTransformation() {
        preparePrevDnBCacheSeed();
        prepareNewDnBCacheSeed();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("DnBAddMissingCols");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<String>();
        baseSources.add(baseSource1.getSourceName());
        baseSources.add(baseSource2.getSourceName());
        step1.setBaseSources(baseSources);
        step1.setTransformer(DnbMissingColsAddFromPrevFlow.TRANSFORMER_NAME);
        step1.setTargetSource(source.getSourceName());
        String confParamStr1 = getDnbAddMissingColsConfig();
        step1.setConfiguration(confParamStr1);

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step1);

        // -----------
        configuration.setSteps(steps);

        return configuration;
    }

    private String getDnbAddMissingColsConfig() {
        DnBAddMissingColsConfig conf = new DnBAddMissingColsConfig();
        conf.setDomain("LE_DOMAIN");
        conf.setDuns("DUNS_NUMBER");
        conf.setFieldsToRemove(Arrays.asList("LE_Last_Upload_Date", "LE_NUMBER_OF_LOCATIONS"));
        conf.setFieldsToPopulateNull(Arrays.asList("SALES_VOLUME_RELIABILITY_CODE", "LE_INDUSTRY"));
        return JsonUtils.serialize(conf);
    }

    // ID, LE_DOMAIN, DUNS_NUMBER, SALES_VOLUME_US_DOLLARS,
    // SALES_VOLUME_RELIABILITY_CODE, LE_INDUSTRY,
    // LE_NUMBER_OF_LOCATIONS, LE_Last_Upload_Date
    private Object[][] data1 = new Object[][] { //
            { 1, "abc.com", "1234", 0L, "1", "GHI", 1, 1234L }, //
            { 2, "def.com", "5678", 0L, "2", "KLM", 2, 2345L }, //
            { 3, "abc.com", "3333", 1L, "3", "STD", 3, 3456L }, //
            { 4, "ghi.com", "5678", 1L, "3", "KLM", 1, 4567L }, //
            { 5, "jkl.com", "9101", 1L, "4", "EFG", 0, 1L }, //
            { 6, "mno.com", "1011", 1L, "4", "FGH", 4, 5678L }, //
            { 7, null, null, 0L, "5", "YYY", 1, 6789L }, //
            { 8, null, "9101", 0L, "1", "JJJ", 2, 0L }, //
            { 9, "ste.com", "1413", 1L, "1", "GFG", 3, 7890L }, //
            { 10, "kkk.com", "555", 0L, "2", "YGH", 4, 9990L }, //
    };

    // ID, LE_DOMAIN, DUNS_NUMBER, SALES_VOLUME_US_DOLLARS,
    // SALES_VOLUME_RELIABILITY_CODE, EMPLOYEES_TOTAL,
    // EMPLOYEES_TOTAL_RELIABILITY_CODE,
    // EMPLOYEES_HERE, EMPLOYEES_HERE_RELIABILITY_CODE
    private Object[][] data2 = new Object[][] { //
            { 1, "abc.com", "1234", 0L, "12", 1, "01", 1, "01" }, //
            { 2, null, null, 0L, "20", 1, "02", 1, "02" }, //
            { 3, null, "9101", 1L, "15", 0, "03", 1, "03" }, //
            { 4, "mno.com", null, 1L, "16", 0, "04", 1, "04" }, //
            { 5, "mno.com", "1011", 1L, "15", 1, "05", 0, "05" }, //
            { 6, "ste.com", null, 1L, "14", 1, "02", 0, "02" }, //
            { 7, "uvw.com", "1112", 0L, "13", 0, "06", 0, "06" }, //
            { 8, "def.com", "5678", 0L, "12", 0, "05", 0, "05" }, //
            { 9, null, null, 1L, "11", 20, "03", 20, "03" }, //
    };

    private void preparePrevDnBCacheSeed() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("ID", Integer.class));
        columns.add(Pair.of("LE_DOMAIN", String.class));
        columns.add(Pair.of("DUNS_NUMBER", String.class));
        columns.add(Pair.of("SALES_VOLUME_US_DOLLARS", Long.class));
        columns.add(Pair.of("SALES_VOLUME_RELIABILITY_CODE", String.class));
        columns.add(Pair.of("LE_INDUSTRY", String.class));
        columns.add(Pair.of("LE_NUMBER_OF_LOCATIONS", Integer.class));
        columns.add(Pair.of("LE_Last_Upload_Date", Long.class));
        uploadBaseSourceData(baseSource1.getSourceName(), baseSourceVersion, columns, data1);
    }

    private void prepareNewDnBCacheSeed() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("ID", Integer.class));
        columns.add(Pair.of("LE_DOMAIN", String.class));
        columns.add(Pair.of("DUNS_NUMBER", String.class));
        columns.add(Pair.of("SALES_VOLUME_US_DOLLARS", Long.class));
        columns.add(Pair.of("SALES_VOLUME_RELIABILITY_CODE", String.class));
        columns.add(Pair.of("EMPLOYEES_TOTAL", Integer.class));
        columns.add(Pair.of("EMPLOYEES_TOTAL_RELIABILITY_CODE", String.class));
        columns.add(Pair.of("EMPLOYEES_HERE", Integer.class));
        columns.add(Pair.of("EMPLOYEES_HERE_RELIABILITY_CODE", String.class));
        uploadBaseSourceData(baseSource2.getSourceName(), baseSourceVersion, columns, data2);
    }

    // ID, LE_DOMAIN, DUNS_NUMBER, SALES_VOLUME_US_DOLLARS,
    // SALES_VOLUME_RELIABILITY_CODE, EMPLOYEES_TOTAL,
    // EMPLOYEES_TOTAL_RELIABILITY_CODE,
    // EMPLOYEES_HERE,EMPLOYEES_HERE_RELIABILITY_CODE,LE_INDUSTRY
    private Object[][] expectedData = new Object[][] { { 1, "abc.com", "1234", 0L, null, 1, "01", 1, "01", null }, //
            { 2, null, null, 0L, null, 1, "02", 1, "02", null }, //
            { 3, null, "9101", 1L, null, 0, "03", 1, "03", null }, //
            { 4, "mno.com", null, 1L, null, 0, "04", 1, "04", null }, //
            { 5, "mno.com", "1011", 1L, null, 1, "05", 0, "05", null }, //
            { 6, "ste.com", null, 1L, null, 1, "02", 0, "02", null }, //
            { 7, "uvw.com", "1112", 0L, null, 0, "06", 0, "06", null }, //
            { 8, "def.com", "5678", 0L, null, 0, "05", 0, "05", null }, //
            { 9, null, null, 1L, null, 20, "03", 20, "03", null }, //
    };

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        Map<Integer, Object[]> expectedMap = new HashMap<>();
        for (Object[] data : expectedData) {
            expectedMap.put((Integer) data[0], data);
        }
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Object[] expectedResult = expectedMap.get(record.get("ID"));
            Assert.assertTrue(isObjEquals(record.get("LE_DOMAIN"), expectedResult[1]));
            Assert.assertTrue(isObjEquals(record.get("DUNS_NUMBER"), expectedResult[2]));
            Assert.assertTrue(isObjEquals(record.get("SALES_VOLUME_US_DOLLARS"), expectedResult[3]));
            Assert.assertTrue(isObjEquals(record.get("SALES_VOLUME_RELIABILITY_CODE"), expectedResult[4]));
            Assert.assertTrue(isObjEquals(record.get("EMPLOYEES_TOTAL"), expectedResult[5]));
            Assert.assertTrue(isObjEquals(record.get("EMPLOYEES_TOTAL_RELIABILITY_CODE"), expectedResult[6]));
            Assert.assertTrue(isObjEquals(record.get("EMPLOYEES_HERE"), expectedResult[7]));
            Assert.assertTrue(isObjEquals(record.get("EMPLOYEES_HERE_RELIABILITY_CODE"), expectedResult[8]));
            Assert.assertTrue(isObjEquals(record.get("LE_INDUSTRY"), expectedResult[9]));
            rowNum++;
        }
        Assert.assertEquals(rowNum, expectedMap.size());
    }

}
