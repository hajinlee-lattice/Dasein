package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.AccountMaster;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.SourceBucketer;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.SourceSorter;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AccountMasterBucketTestNG extends PipelineTransformationTestNGBase {

    private static final Log log = LogFactory.getLog(AccountMasterBucketTestNG.class);

    @Autowired
    private AccountMaster accountMaster;

    @Test(groups = "functional", enabled = true)
    public void testTransformation() throws Exception {
        uploadBaseSourceFile(accountMaster, "AM_TestAMBucket", baseSourceVersion);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        verifyFileSplitting();
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return "AccountMasterBucketed";
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(accountMaster.getSourceName(), baseSourceVersion).toString();
    }

    @Override
    PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("AccountMasterBucket");
            configuration.setVersion(targetVersion);
            // -----------
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = Collections.singletonList(accountMaster.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(SourceBucketer.TRANSFORMER_NAME);
            step1.setConfiguration("{}");
            // -----------
            TransformationStepConfig step2 = new TransformationStepConfig();
            step2.setInputSteps(Collections.singletonList(0));
            step2.setTransformer(SourceSorter.TRANSFORMER_NAME);
            step2.setConfiguration(sortStepConfiguration());
            // -----------
            step2.setTargetSource(getTargetSourceName());
            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(step1);
            steps.add(step2);
            // -----------
            configuration.setSteps(steps);
            configuration.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
            return configuration;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        // correctness is tested in the dataflow functional test
        log.info("Start to verify records one by one.");
        int rowCount = 0;
        boolean hasNotZero = false;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            for (Schema.Field field : record.getSchema().getFields()) {
                if (!hasNotZero && field.name().startsWith("EAttr")) {
                    long value = (Long) record.get(field.pos());
                    if (value != 0) {
                        hasNotZero = true;
                    }
                }
            }
            rowCount++;
        }
        Assert.assertTrue(hasNotZero);
        Assert.assertEquals(rowCount, 1000);
    }

    private String sortStepConfiguration() throws IOException {
        SorterConfig config = new SorterConfig();
        config.setPartitions(100);
        config.setSortingField("LatticeAccountId");
        return JsonUtils.serialize(config);
    }

    private void verifyFileSplitting() throws IOException {
        String resultDir = getPathForResult();
        List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration, resultDir + "/*.avro");
        long maxInLastFile = Integer.MIN_VALUE;
        for (String file : files) {
            long minInFile = Integer.MAX_VALUE;
            long maxInFile = Integer.MIN_VALUE;
            List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration, file);
            for (GenericRecord record : records) {
                long id = (long) record.get("LatticeAccountId");
                minInFile = Math.min(id, minInFile);
                maxInFile = Math.max(id, maxInFile);
            }
            String fileName = file.substring(file.lastIndexOf("/") + 1);
            System.out.println(String.format("[%s] Min: %d -- Max: %d", fileName, minInFile, maxInFile));
            Assert.assertTrue(minInFile > maxInLastFile);
            maxInLastFile = maxInFile;
        }
    }

}
