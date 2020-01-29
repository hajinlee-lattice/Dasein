package com.latticeengines.datacloud.etl.transformation.service.impl.minidc;

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
import com.latticeengines.datacloud.dataflow.transformation.minidc.MiniAMSeedSampleSetFlow;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.minidc.MiniAMSeedSampleSetConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class MiniAMSeedSampleSetTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(MiniAMSeedSampleSetTestNG.class);
    private GeneralSource source = new GeneralSource("MiniAMSeedSampleSet");
    private GeneralSource baseSource = new GeneralSource("MiniAMDataSet");
    private GeneralSource baseSourceDataSeed = new GeneralSource("SeedDataSet");

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        prepareMiniAmDataSet();
        prepareSeedDataSet();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("MiniAMSampleSet");
        configuration.setVersion(targetVersion);

        // Initialize Sampled Data Set
        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSourcesStep1 = new ArrayList<>();
        baseSourcesStep1.add(baseSource.getSourceName());
        baseSourcesStep1.add(baseSourceDataSeed.getSourceName());
        step1.setBaseSources(baseSourcesStep1);
        step1.setTransformer(MiniAMSeedSampleSetFlow.TRANSFORMER);
        String confParamStr1 = getMiniAMSampledSetConfig();
        step1.setConfiguration(confParamStr1);
        step1.setTargetSource(source.getSourceName());

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(step1);

        // -----------
        configuration.setSteps(steps);

        return configuration;
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        Map<String, List<String>> expectedData = new HashMap<String, List<String>>();
        String[][] expectedRecord = {
                { "intel.com", "3456789012", "Intel", "Santa Clara", "California", "United States", "2"},
                { "google.com", "123456789", "Google", "Mountain View", "California", "United States", "0"},
                { "netapp.com", "234567890", "Netapp", "Sunnyvale", "California", "United States", "1" } };
        for (String[] value : expectedRecord) {
            expectedData.put(value[6], Arrays.asList(value));
        }
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String uniqueId = record.get("Id").toString();
            log.info("record : " + record);
            log.info("uniqueId : " + uniqueId);
            Assert.assertTrue(expectedData.get(uniqueId).get(0).contains(record.get("Domain").toString())
                    && expectedData.get(uniqueId).get(1).contains(record.get("Duns").toString())
                    && expectedData.get(uniqueId).get(2).contains(record.get("Name").toString())
                    && expectedData.get(uniqueId).get(3).contains(record.get("City").toString())
                    && expectedData.get(uniqueId).get(4).contains(record.get("State").toString())
                    && expectedData.get(uniqueId).get(5).contains(record.get("Country").toString()));
            rowCount++;
        }
        log.info("rowCount : " + rowCount);
        Assert.assertEquals(rowCount, 3);
    }

    private void prepareMiniAmDataSet() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Type", String.class));
        columns.add(Pair.of("Value", String.class));
        Object[][] data = new Object[][] { { "Duns", "234567890" },
                { "Domain", "citrix.com" }, { "Domain", "google.com" }, { "Duns", "4567890123" },
                { "Duns", "987624588" }, { "Domain", "intel.com" }, { "Domain", "salesforce.com" },
                { "Duns", "123456789" } };
        uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, columns, data);
    }

    private void prepareSeedDataSet() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("Duns", String.class));
        columns.add(Pair.of("Name", String.class));
        columns.add(Pair.of("City", String.class));
        columns.add(Pair.of("State", String.class));
        columns.add(Pair.of("Country", String.class));
        columns.add(Pair.of("Id", String.class));
        Object[][] data = new Object[][] {
                { "google.com", "123456789", "Google", "Mountain View", "California", "United States", "0" },
                { "netapp.com", "234567890", "Netapp", "Sunnyvale", "California", "United States", "1" },
                { "intel.com", "3456789012", "Intel", "Santa Clara", "California", "United States", "2" },
                { "amazon.com", "8901234567", "Amazon", "Seattle", "Washington", "United States", "3" },
                { "facebook.com", "9012345678", "Facebook", "Menlo Park", "California", "United States", "4" } };
        uploadBaseSourceData(baseSourceDataSeed.getSourceName(), baseSourceVersion, columns, data);
    }

    private String getMiniAMSampledSetConfig() {
        MiniAMSeedSampleSetConfig conf = new MiniAMSeedSampleSetConfig();
        conf.setMiniDataSetType("Type");
        conf.setMiniDataSetValue("Value");
        conf.setSampledSetDomain("Domain");
        conf.setSampledSetDuns("Duns");
        List<String> columnList = new ArrayList<>();
        columnList.add(conf.getSampledSetDomain());
        columnList.add(conf.getSampledSetDuns());
        conf.setKeyIdentifier(columnList);
        return JsonUtils.serialize(conf);
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

}
