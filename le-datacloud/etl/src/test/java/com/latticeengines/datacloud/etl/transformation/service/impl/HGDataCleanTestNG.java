package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.HGDataCleanFlow;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.HGDataCleanConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class HGDataCleanTestNG extends PipelineTransformationTestNGBase{

    private static final Logger log = LoggerFactory.getLogger(HGDataCleanTestNG.class);

    GeneralSource baseSource = new GeneralSource("HGSeedRaw");

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "pipeline2")
    public void testTransformation() {
        uploadBaseAvro(baseSource, baseSourceVersion);
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
            step1.setConfiguration(setDataFlowEngine(confParamStr1, "TEZ"));

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

    private String getHGDataCleanConfig() throws JsonProcessingException {
        HGDataCleanConfig config = new HGDataCleanConfig();
        config.setDomainField("URL");
        calendar.set(2016, Calendar.AUGUST, 1);
        config.setFakedCurrentDate(calendar.getTime());
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

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int recordsToCheck = 100;
        int pos = 0;
        Long sixMonths = 6 * TimeUnit.DAYS.toMillis(30);
        while (pos++ < recordsToCheck && records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Long lastVerified = (Long) record.get("Last_Verified_Date");
            Long timeStamp = (Long) record.get("LE_Last_Upload_Date");
            try {
                Assert.assertTrue(timeStamp < lastVerified + sixMonths);
            } catch (Exception e) {
                System.out.println(record);
                throw e;
            }
        }
    }

}
