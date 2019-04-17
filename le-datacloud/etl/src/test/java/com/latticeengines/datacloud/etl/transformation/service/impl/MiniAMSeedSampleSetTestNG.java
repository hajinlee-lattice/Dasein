package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
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
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.MiniAMSeedSampleSetConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class MiniAMSeedSampleSetTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(MiniAMSeedSampleSetTestNG.class);
    GeneralSource source = new GeneralSource("MiniAMSeedSampleSet");

    GeneralSource baseSource = new GeneralSource("MiniAMDataSet");
    GeneralSource baseSourceDataSeed = new GeneralSource("SeedDataSet");

    String targetSourceName = source.getSourceName();

    ObjectMapper om = new ObjectMapper();

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
    protected TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    protected Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("MiniAMSampleSet");
            configuration.setVersion(targetVersion);

            // Initialize Sampled Data Set
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourcesStep1 = new ArrayList<String>();
            baseSourcesStep1.add(baseSource.getSourceName());
            baseSourcesStep1.add(baseSourceDataSeed.getSourceName());
            step1.setBaseSources(baseSourcesStep1);
            step1.setTransformer("miniAMSampledSetTransformer");
            String confParamStr1 = getMiniAMSampledSetConfig();
            step1.setConfiguration(confParamStr1);
            step1.setTargetSource(targetSourceName);

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);

            // -----------
            configuration.setSteps(steps);

            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
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

    private String getMiniAMSampledSetConfig() throws JsonProcessingException {
        MiniAMSeedSampleSetConfig conf = new MiniAMSeedSampleSetConfig();
        conf.setMiniDataSetType("Type");
        conf.setMiniDataSetValue("Value");
        conf.setSampledSetDomain("Domain");
        conf.setSampledSetDuns("Duns");
        List<String> columnList = new ArrayList<String>();
        columnList.add(conf.getSampledSetDomain());
        columnList.add(conf.getSampledSetDuns());
        conf.setKeyIdentifier(columnList);
        return om.writeValueAsString(conf);
    }

}
