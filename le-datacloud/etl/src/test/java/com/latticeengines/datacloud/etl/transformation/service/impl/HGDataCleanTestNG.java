package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.HGDataCleanFlow;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.HGDataCleanConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class HGDataCleanTestNG extends PipelineTransformationTestNGBase{

    private static final Logger log = LoggerFactory.getLogger(HGDataCleanTestNG.class);

    GeneralSource baseSource = new GeneralSource("HGSeedRaw");

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "pipeline2")
    public void testTransformation() {
        prepareHGSeedRaw();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return "HGDataClean";
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(getTargetSourceName(), targetVersion).toString();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("HGDataClean");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add(baseSource.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(HGDataCleanFlow.TRANSFORMER_NAME);
            step1.setTargetSource(getTargetSourceName());
            String confParamStr1 = getHGDataCleanConfig();
            // step1.setConfiguration(setDataFlowEngine(confParamStr1, "TEZ"));
            step1.setConfiguration(confParamStr1);

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(step1);

            // -----------
            configuration.setSteps(steps);

            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getHGDataCleanConfig() throws JsonProcessingException {
        HGDataCleanConfig config = new HGDataCleanConfig();
        config.setDomainField("URL");
        config.setDateLastVerifiedField("DateLastVerified");
        config.setVendorField("Vendor");
        config.setProductField("Product");
        config.setCategoryField("Category");
        config.setCategory2Field("Category2");
        config.setCategoryParentField("CategoryParent");
        config.setCategoryParent2Field("CategoryParent2");
        config.setIntensityField("Intensity");
        return om.writeValueAsString(config);
    }

    private void prepareHGSeedRaw() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("URL", String.class));
        columns.add(Pair.of("Company", String.class));
        columns.add(Pair.of("City", String.class));
        columns.add(Pair.of("State", String.class));
        columns.add(Pair.of("Country", String.class));
        columns.add(Pair.of("ProductID", String.class));
        columns.add(Pair.of("Vendor", String.class));
        columns.add(Pair.of("Product", String.class));
        columns.add(Pair.of("DateLastVerified", String.class));
        columns.add(Pair.of("Intensity", String.class));
        columns.add(Pair.of("CategoryParent", String.class));
        columns.add(Pair.of("Category", String.class));
        columns.add(Pair.of("CategoryParent2", String.class));
        columns.add(Pair.of("Category2", String.class));
        columns.add(Pair.of("Attributes", String.class));

        // When filter by DateLastVerified, compare with 2018-01-01
        Object[][] data = new Object[][] { //
                // Case 1: No domain, to remove; DateLastVerified will be used
                // to compare
                { null, "Comp1", "CT1", "ST1", "CTRY1", "PID1", "VD1", "PRD1", "2018-01-01", "1", "CATPT_1", "CAT_1",
                        "CATPT2_1", "CAT2_1", "ATTR1" }, //

                // Case 2: Null Intensity
                { "dom2.com", "Comp2", "CT2", "ST2", "CTRY2", "PID2", "VD2", "PRD2", "2017-12-01", null, "CATPT_2",
                        "CAT_2", "CATPT2_2", "CAT2_2", "ATTR2" }, //

                // Case 3: Old DateLastVerified, to remove
                { "dom3.com", "Comp3", "CT3", "ST3", "CTRY3", "PID3", "VD3", "PRD3", "2017-06-15", "1", "CATPT_3",
                        "CAT_3", "CATPT2_3", "CAT2_3", "ATTR3" }, //

                // Case 4: Invalid domain, to remove
                { "dom4", "Comp4", "CT4", "ST4", "CTRY4", "PID4", "VD4", "PRD4", "2017-12-01", "1", "CATPT_4", "CAT_4",
                        "CATPT2_4", "CAT2_4", "ATTR4" }, //

                // Case 5: Multi-location, only retain the one with latest
                // DateLastVerified; Verify calculation of
                // Max_Location_Intensity and Location_Count
                { "dom5.com", "Comp5", "CT5", "ST5", "CTRY5", "PID5", "VD5", "PRD5", "2017-11-15", "1", "CATPT_5",
                        "CAT_5", "CATPT2_5", "CAT2_5", "ATTR5" }, //
                { "dom5.com", "Comp5", "CT5", "ST5", "CTRY5", "PID5", "VD5", "PRD5", "2017-11-01", "2", "CATPT_5",
                        "CAT_5", "CATPT2_5", "CAT2_5", "ATTR5" }, //
                // Old DateLastVerifiedDate, but intensity is chosen as
                // Max_Location_Intensity
                { "dom5.com", "Comp5", "CT5", "ST5", "CTRY5", "PID5", "VD5", "PRD5", "2016-01-01", "3", "CATPT_5",
                        "CAT_5", "CATPT2_5", "CAT2_5", "ATTR5" }, //

                // Case 6: One domain, multi-techs; partially null Intensity
                { "dom6.com", "Comp6", "CT6", "ST6", "CTRY6", "PID6", "VD6", "PRD6", "2017-12-02", null, "CATPT_6",
                        "CAT_6", "CATPT2_6", "CAT2_6", "ATTR6" }, //
                // To remove, not latest DateLastVerified
                { "dom6.com", "Comp6", "CT6", "ST6", "CTRY6", "PID6", "VD6", "PRD6", "2017-12-01", null, "CATPT_6",
                        "CAT_6", "CATPT2_6", "CAT2_6", "ATTR6" }, //
                { "dom6.com", "Comp6", "CT6", "ST6", "CTRY6", "PID66", "VD66", "PRD66", "2017-11-01", "3", "CATPT_66",
                        "CAT_66", "CATPT2_66", "CAT2_66", "ATTR66" }, //
                // To remove, old DateLastVerified
                { "dom6.com", "Comp6", "CT6", "ST6", "CTRY6", "PID666", "VD666", "PRD666", "2016-11-01", "1",
                        "CATPT_666", "CAT_666", "CATPT2_666", "CAT2_666", "ATTR666" }, //
                // To remove, null DateLastVerified
                { "dom6.com", "Comp6", "CT6", "ST6", "CTRY6", "PID6666", "VD6666", "PRD6666", null, null, "CATPT_6666",
                        "CAT_6666", "CATPT2_6666", "CAT2_6666", "ATTR6666" }, //
                // To remove, invalid DateLastVerified
                { "dom6.com", "Comp6", "CT6", "ST6", "CTRY6", "PID66666", "VD66666", "PRD66666", "invalid timestamp",
                        null, "CATPT_66666", "CAT_66666", "CATPT2_66666", "CAT2_66666", "ATTR66666" }, //
        };
        uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, columns, data);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        // Domain, Supplier_Name, Segment_Name, HG_Category_1, HG_Category_2,
        // HG_Category_1_Parent, HG_Category_2_Parent, Last_Verified_Date,
        // Max_Location_Intensity, Location_Counts
        Object[][] expected = new Object[][] { //
                { "dom2.com", "VD2", "PRD2", "CAT_2", "CAT2_2", "CATPT_2", "CATPT2_2", "2017-12-01", null, 1 }, //
                { "dom5.com", "VD5", "PRD5", "CAT_5", "CAT2_5", "CATPT_5", "CATPT2_5", "2017-11-15", 3, 3 }, //
                { "dom6.com", "VD6", "PRD6", "CAT_6", "CAT2_6", "CATPT_6", "CATPT2_6", "2017-12-02", null, 2 }, //
                { "dom6.com", "VD66", "PRD66", "CAT_66", "CAT2_66", "CATPT_66", "CATPT2_66", "2017-11-01", 3, 1 }, //
        };
        Map<String, Object[]> expectedMap = new HashMap<>();
        for (Object[] obj : expected) {
            expectedMap.put(buildId(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6]), obj);
        }
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            String id = buildId(record.get(HGDataCleanFlow.FINAL_DOMAIN), //
                    record.get(HGDataCleanFlow.FINAL_SUPPLIER_NAME), //
                    record.get(HGDataCleanFlow.FINAL_SEGMENT_NAME), //
                    record.get(HGDataCleanFlow.FINAL_CAT1), //
                    record.get(HGDataCleanFlow.FINAL_CAT2), //
                    record.get(HGDataCleanFlow.FINAL_PARENT_CAT1), //
                    record.get(HGDataCleanFlow.FINAL_PARENT_CAT2));
            Assert.assertNotNull(expectedMap.get(id));
            Object[] expectedRecord = expectedMap.get(id);
            Assert.assertTrue(isObjEquals(record.get(HGDataCleanFlow.FINAL_DOMAIN), expectedRecord[0]));
            Assert.assertTrue(isObjEquals(record.get(HGDataCleanFlow.FINAL_SUPPLIER_NAME), expectedRecord[1]));
            Assert.assertTrue(isObjEquals(record.get(HGDataCleanFlow.FINAL_SEGMENT_NAME), expectedRecord[2]));
            Assert.assertTrue(isObjEquals(record.get(HGDataCleanFlow.FINAL_CAT1), expectedRecord[3]));
            Assert.assertTrue(isObjEquals(record.get(HGDataCleanFlow.FINAL_CAT2), expectedRecord[4]));
            Assert.assertTrue(isObjEquals(record.get(HGDataCleanFlow.FINAL_PARENT_CAT1), expectedRecord[5]));
            Assert.assertTrue(isObjEquals(record.get(HGDataCleanFlow.FINAL_PARENT_CAT2), expectedRecord[6]));
            try {
                Assert.assertTrue(isObjEquals(record.get(HGDataCleanFlow.FINAL_LAST_VERIFIED_DATE),
                        df.parse(String.valueOf(expectedRecord[7])).getTime()));
            } catch (ParseException e) {
                log.error("Fail to parse " + String.valueOf(expectedRecord[7]), e);
                Assert.assertTrue(false);
            }
            Assert.assertTrue(isObjEquals(record.get(HGDataCleanFlow.FINAL_MAX_LOC_INTENSITY), expectedRecord[8]));
            Assert.assertTrue(isObjEquals(record.get(HGDataCleanFlow.FINAL_LOC_COUNT), expectedRecord[9]));
        }
    }

    private String buildId(Object domain, Object supplier, Object segment, Object cat1, Object cat2, Object parentCat1,
            Object parentCat2) {
        return String.valueOf(domain) + String.valueOf(supplier) + String.valueOf(segment) + String.valueOf(cat1)
                + String.valueOf(cat2) + String.valueOf(parentCat1) + String.valueOf(parentCat2);
    }

}
