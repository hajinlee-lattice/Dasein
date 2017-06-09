package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.HasSqlPresence;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.FeatureMostRecent;
import com.latticeengines.datacloud.core.source.impl.FeaturePivoted;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PivotConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class FeaturePivotServiceImplTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private final static String TRANSFORMER_NAME = "featurePivotFlowTransformer";

    @Autowired
    FeaturePivoted source;

    @Autowired
    FeatureMostRecent baseSource;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "pipeline2", enabled = true)
    public void testTransformation() {
        uploadBaseAvro(baseSource, baseSourceVersion);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    Source getSource() {
        return source;
    }

    @Override
    String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("FeaturePivoted");
            configuration.setVersion(targetVersion);

            // Initialize FeatureMostRecent
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourceStep1 = new ArrayList<String>();
            baseSourceStep1.add(baseSource.getSourceName());
            step1.setBaseSources(baseSourceStep1);
            step1.setTargetSource(source.getSourceName());
            step1.setTransformer(TRANSFORMER_NAME);
            String confParamStr1 = getFeatureMostRecentConfig();
            step1.setConfiguration(confParamStr1);
            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            // -----------
            configuration.setSteps(steps);
            return configuration;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String getFeatureMostRecentConfig() throws JsonProcessingException {
        PivotConfig conf = new PivotConfig();
        conf.setJoinFields(source.getPrimaryKey());
        conf.setHasSqlPresence(source instanceof HasSqlPresence);
        return om.writeValueAsString(conf);
    }

    @Override
    String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        Object[][] expectedData = new Object[][] { { "hpsystem.com", 26766, 26766, 11, 55, 0 },
                { "primrosehillschool.com", 67978, 67978, 8, 83, 0 },
                { "laxfinancialconsulting.com", 119379, 119379, 29, 40, 1691 } };
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Object url = record.get("URL");
            Object averageDocumentSizeFetched = record.get("Average_Document_Size_Fetched");
            Object averageDocumentSizeProcessed = record.get("Average_Document_Size_Processed");
            Object executionFull = record.get("Execution_Full");
            Object termStore = record.get("Term_Store");
            Object currencyUsDollar = record.get("Currency_US_Dollar");
            int index = 0;
            for (int i = 0; i < expectedData.length; i++) {
                if ((url.toString()).equals(expectedData[i][0])) {
                    index = i;
                    break;
                }
            }
            Assert.assertEquals(url.toString(), expectedData[index][0]);
            Assert.assertEquals(Integer.parseInt(averageDocumentSizeFetched.toString()), expectedData[index][1]);
            Assert.assertEquals(Integer.parseInt(averageDocumentSizeProcessed.toString()), expectedData[index][2]);
            Assert.assertEquals(Integer.parseInt(executionFull.toString()), expectedData[index][3]);
            Assert.assertEquals(Integer.parseInt(termStore.toString()), expectedData[index][4]);
            Assert.assertEquals(Integer.parseInt(currencyUsDollar.toString()), expectedData[index][5]);
            rowCount++;
        }
        Assert.assertEquals(rowCount, 3);
    }
}
