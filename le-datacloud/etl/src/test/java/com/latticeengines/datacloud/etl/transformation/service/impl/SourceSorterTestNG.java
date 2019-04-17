package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.AccountMaster;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.SourceSorter;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class SourceSorterTestNG extends PipelineTransformationTestNGBase {

    private static final int NUM_ROWS = 80;
    private static final int PARTITIONS = 16;

    @Autowired
    private AccountMaster accountMaster;

    @Test(groups = "functional")
    public void testTransformation() throws Exception {
        prepareAM();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        verifyFileSplitting();
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return "AMSorted";
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(accountMaster.getSourceName(), baseSourceVersion).toString();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("SortAM");
            configuration.setVersion(targetVersion);
            // -----------
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add(accountMaster.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(SourceSorter.TRANSFORMER_NAME);
            step1.setTargetSource(getTargetSourceName());
            step1.setConfiguration(stepConfiguration());
            // -----------
            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(step1);
            // -----------
            configuration.setSteps(steps);

            configuration.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
            return configuration;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {}

    private void verifyFileSplitting() throws IOException {
        String resultDir = getPathForResult();
        List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration, resultDir + "/*.avro");
        long maxInLastFile = Integer.MIN_VALUE;
        for (String file: files) {
            long minInFile = Integer.MAX_VALUE;
            long maxInFile = Integer.MIN_VALUE;
            List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration, file);
            for (GenericRecord record: records) {
                //System.out.println(record);
                long id = (long) record.get("LatticeId");
                minInFile = Math.min(id, minInFile);
                maxInFile = Math.max(id, maxInFile);
            }
            String fileName = file.substring(file.lastIndexOf("/") + 1);
            System.out.println(String.format("[%s] Min: %d -- Max: %d", fileName, minInFile, maxInFile));
            Assert.assertTrue(minInFile > maxInLastFile);
            maxInLastFile = maxInFile;
        }
    }

    private String stepConfiguration() throws IOException {
        SorterConfig config = new SorterConfig();
        config.setPartitions(PARTITIONS);
        config.setSortingField("LatticeId");
        String conf = JsonUtils.serialize(config);
        TransformationFlowParameters.EngineConfiguration engineConfiguration = new TransformationFlowParameters.EngineConfiguration();
        engineConfiguration.setEngine("FLINK");
        return setDataFlowEngine(conf, engineConfiguration);
    }

    private void prepareAM() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("LatticeId", Long.class), //
                Pair.of("Field", String.class)
        );
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < NUM_ROWS; i++) {
            ids.add((long) (i + 1));
        }
        Collections.shuffle(ids);
        Object[][] data = new Object[NUM_ROWS][2];
        for (int i = 0; i < NUM_ROWS; i++) {
            data[i][0] = ids.get(i);
            data[i][1] = String.format("%d-%s", ids.get(i), UUID.randomUUID().toString().substring(0, 8));
        }

        uploadBaseSourceData(accountMaster.getSourceName(), baseSourceVersion, fields, data);
    }

}
