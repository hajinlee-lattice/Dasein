package com.latticeengines.datacloud.etl.transformation.service.impl.source;

import java.util.ArrayList;
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

import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.source.DMXDataCleanFlow;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.source.DMXDataCleanConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public class DMXDataCleanTestNG extends PipelineTransformationTestNGBase {
    
    private static final Logger log = LoggerFactory.getLogger(DMXDataCleanTestNG.class);
    
    private GeneralSource baseSource = new GeneralSource("DMXSeedRaw");

    @Test(groups = "pipeline2")
    public void testTransformation() {
        prepareDMXSeedRaw();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }
    
    @Override
    protected String getTargetSourceName() {
        return "DMXDataClean";
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("DMXDataClean");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(baseSource.getSourceName());
        step1.setBaseSources(baseSources);
        step1.setTransformer(DMXDataCleanFlow.TRANSFORMER_NAME);
        step1.setTargetSource(getTargetSourceName());
        String confParamStr1 = getDMXDataCleanConfig();
        step1.setConfiguration(confParamStr1);

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(step1);

        // -----------
        configuration.setSteps(steps);
        return configuration;
    }
    
    private String getDMXDataCleanConfig() {
        DMXDataCleanConfig config = new DMXDataCleanConfig();
        config.setDunsField("Duns");
        config.setVendorField("Vendor");
        config.setProductField("Product");
        config.setCategoryField("Category");
        config.setRecordTypeField("RecordType");
        config.setRecordValueField("RecordValue");
        config.setDescriptionField("Description");
        config.setIntensityField("intensity");
        return JsonUtils.serialize(config);
    }

    private void prepareDMXSeedRaw() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Duns", String.class));
        columns.add(Pair.of("Website", String.class));
        columns.add(Pair.of("RecordType", String.class));
        columns.add(Pair.of("RecordValue", String.class));
        columns.add(Pair.of("Category", String.class));
        columns.add(Pair.of("Vendor", String.class));
        columns.add(Pair.of("Product", String.class));
        columns.add(Pair.of("intensity", String.class));
        columns.add(Pair.of("Description", String.class));

        // When filter by DateLastVerified, compare with 2018-01-01
        Object[][] data = new Object[][] { //
                // Record Type = P
                { "060902413", null, "P", null, "OFFICE", "google.com", "GOOGLE MAPS", "High", "GOOGLE MAPS DESCRIPTION" }, //

                // Record Type = V
                { "884745530", "amzn.com", "V", "amazon.com", "VENDOR PRESENCE", null, null, "High", "AMAZON DESCRIPTION" }, //

                // Record Type = S
                { "072148831", null, "S", "DATABASE SOFTWARE SOLN", "IT", null, null, "Low", "DATABASE SOFTWARE SOLN DESCRIPTION" }, //
        };
        uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, columns, data);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        while (records.hasNext()) {            
            log.info("Start to verify records one by one.");
            // Duns, Intensity, Segment_Name, Supplier_Name, Collection_Name
            Object[][] expected = new Object[][] { //
                    { "884745530", "High", null, "amazon.com", null }, //
                    { "060902413", "High", "GOOGLE MAPS", null, null }, //
                    { "072148831", "Low", null, null, "DATABASE SOFTWARE SOLN" }, //
            };
            Map<String, Object[]> expectedMap = new HashMap<>();
            for (Object[] obj : expected) {
                expectedMap.put(buildId(obj[0], obj[1], obj[2], obj[3], obj[4]), obj);
            }
            while (records.hasNext()) {
                GenericRecord record = records.next();
                log.info(record.toString());
                String id = buildId(record.get(DMXDataCleanFlow.FINAL_DUNS), //
                        record.get(DMXDataCleanFlow.FINAL_INTENSITY), //
                        record.get(DMXDataCleanFlow.FINAL_SEGMENT_NAME), //
                        record.get(DMXDataCleanFlow.FINAL_SUPPLIER_NAME), //
                        record.get(DMXDataCleanFlow.FINAL_COLLECTION_NAME));
                Assert.assertNotNull(expectedMap.get(id));
                Object[] expectedRecord = expectedMap.get(id);
                Assert.assertTrue(isObjEquals(record.get(DMXDataCleanFlow.FINAL_DUNS), expectedRecord[0]));
                Assert.assertTrue(isObjEquals(record.get(DMXDataCleanFlow.FINAL_INTENSITY), expectedRecord[1]));
                Assert.assertTrue(isObjEquals(record.get(DMXDataCleanFlow.FINAL_SEGMENT_NAME), expectedRecord[2]));
                Assert.assertTrue(isObjEquals(record.get(DMXDataCleanFlow.FINAL_SUPPLIER_NAME), expectedRecord[3]));
                Assert.assertTrue(isObjEquals(record.get(DMXDataCleanFlow.FINAL_COLLECTION_NAME), expectedRecord[4]));
            }
        }
    }
    
    private String buildId(Object duns, Object intensity, Object segment, Object supplier, Object collection) {
        return String.valueOf(duns) + String.valueOf(intensity) + String.valueOf(segment) + String.valueOf(supplier) + String.valueOf(collection);
    }

}
