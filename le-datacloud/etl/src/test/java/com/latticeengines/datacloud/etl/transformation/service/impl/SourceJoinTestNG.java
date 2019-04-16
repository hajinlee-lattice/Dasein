package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.Join;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.JoinConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.JoinConfig.JoinType;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class SourceJoinTestNG extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(SourceJoinTestNG.class);

    GeneralSource source = new GeneralSource("JoinedSrc");
    GeneralSource baseSrc1 = new GeneralSource("Src1");
    GeneralSource baseSrc2 = new GeneralSource("Src2");
    GeneralSource baseSrc3 = new GeneralSource("Src3");

    @Test(groups = "pipeline2")
    public void testTransformation() {
        prepareSrc1();
        prepareSrc2();
        prepareSrc3();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("DnBClean");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add(baseSrc1.getSourceName());
            baseSources.add(baseSrc2.getSourceName());
            baseSources.add(baseSrc3.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(Join.TRANSFORMER_NAME);
            step1.setTargetSource(source.getSourceName());
            String confParamStr1 = getTransformerConfig();
            step1.setConfiguration(confParamStr1);

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

    private String getTransformerConfig() throws JsonProcessingException {
        JoinConfig conf = new JoinConfig();
        String[][] joinFields = { //
                { "ID1", "ID2" }, //
                { "ID1", "ID2" }, //
                { "ID1", "ID2" }, //
        };
        conf.setJoinFields(joinFields);
        conf.setJoinType(JoinType.INNER);
        return JsonUtils.serialize(conf);
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
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    private Object[][] data1 = new Object[][] { //
            { 1, 1, "AA" }, //
            { 1, 2, "BB" }, //
    };

    private Object[][] data2 = new Object[][] { //
            { 1, 1, "AAA" }, //
            { 2, 2, "BBB" }, //
    };

    private Object[][] data3 = new Object[][] { //
            { 1, 1, "A" }, //
            { 2, 1, "B" }, //
    };

    private void prepareSrc1() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("ID1", Integer.class));
        columns.add(Pair.of("ID2", Integer.class));
        columns.add(Pair.of("Value", String.class));
        uploadBaseSourceData(baseSrc1.getSourceName(), baseSourceVersion, columns, data1);
    }

    private void prepareSrc2() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("ID1", Integer.class));
        columns.add(Pair.of("ID2", Integer.class));
        columns.add(Pair.of("Value", String.class));
        uploadBaseSourceData(baseSrc2.getSourceName(), baseSourceVersion, columns, data2);
    }

    private void prepareSrc3() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("ID1", Integer.class));
        columns.add(Pair.of("ID2", Integer.class));
        columns.add(Pair.of("Value", String.class));
        uploadBaseSourceData(baseSrc3.getSourceName(), baseSourceVersion, columns, data3);
    }

    private Object[][] expectedData = new Object[][] { //
            { 1, 1, "AA", 1, 1, "AAA", 1, 1, "A" } //
    };

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Assert.assertTrue(isObjEquals(record.get("ID1"), expectedData[0][0]));
            Assert.assertTrue(isObjEquals(record.get("ID2"), expectedData[0][1]));
            Assert.assertTrue(isObjEquals(record.get("Value"), expectedData[0][2]));
            Assert.assertTrue(isObjEquals(record.get("Src2__ID1"), expectedData[0][3]));
            Assert.assertTrue(isObjEquals(record.get("Src2__ID2"), expectedData[0][4]));
            Assert.assertTrue(isObjEquals(record.get("Src2__Value"), expectedData[0][5]));
            Assert.assertTrue(isObjEquals(record.get("Src3__ID1"), expectedData[0][6]));
            Assert.assertTrue(isObjEquals(record.get("Src3__ID2"), expectedData[0][7]));
            Assert.assertTrue(isObjEquals(record.get("Src3__Value"), expectedData[0][8]));
            rowNum++;
        }
        Assert.assertEquals(rowNum, expectedData.length);
    }
}
