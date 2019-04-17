package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.Feature;
import com.latticeengines.datacloud.core.source.impl.FeatureMostRecent;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.dataflow.transformation.MostRecentFlow;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.MostRecentConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class FeatureMostRecentTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    @Autowired
    FeatureMostRecent source;

    @Autowired
    private Feature baseSource;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    ObjectMapper om = new ObjectMapper();

    String baseSourceVersion1 = "2015-11-28_09-44-52_UTC";
    String baseSourceVersion2 = "2015-12-29_10-34-24_UTC";

    @Test(groups = "functional", enabled = true)
    public void testTransformation() {
        uploadBaseSourceFile(source.getBaseSources()[0], "Feature_" + baseSourceVersion1, baseSourceVersion1);
        uploadBaseSourceFile(source.getBaseSources()[0], "Feature_" + baseSourceVersion2, baseSourceVersion2);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected
    TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    protected
    Source getSource() {
        return source;
    }

    @Override
    protected
    String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected
    PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("FeatureMostRecent");
            configuration.setVersion(targetVersion);

            // Initialize CollectedSource
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourceStep1 = new ArrayList<String>();
            baseSourceStep1.add(baseSource.getSourceName());
            step1.setBaseSources(baseSourceStep1);
            step1.setTargetSource(source.getSourceName());
            step1.setTransformer(MostRecentFlow.TRANSFORMER_NAME);
            String confParamStr1 = getMostRecentConfig();
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

    private String getMostRecentConfig() throws JsonProcessingException {
        MostRecentConfig conf = new MostRecentConfig();
        conf.setDomainField("URL");
        conf.setTimestampField("LE_Last_Upload_Date");
        conf.setGroupbyFields(new String[] { "URL", "Feature" });
        conf.setPeriodToKeep(TimeUnit.DAYS.toMillis(365));
        return om.writeValueAsString(conf);
    }

    @Override
    protected
    String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        Object[][] expectedDataCount = new Object[][] { { "primrosehillschool.com", 14 },
                { "limafood.com", 23 } };
        Object[][] expectedDataValues = new Object[][] {
                { 1, "primrosehillschool.com", "Term_Sale", "2", 1449201036757L },
                { 2, "limafood.com", "Term_Catalog", "35", 1449221923163L } };
        int rowCount = 0;
        Map<String, Integer> storeCount = new HashMap<String, Integer>();
        Set<Integer> storeFound = new HashSet<Integer>();
        Iterator<GenericRecord> iterateRecords = records;
        while (records.hasNext()) {
            GenericRecord record = iterateRecords.next();
            String URL = record.get("URL").toString();
            String feature = record.get("Feature").toString();
            String value = record.get("Value").toString();
            Long timestamp = (Long) record.get("Timestamp");
            if (storeCount.containsKey(URL)) {
                storeCount.put(URL, storeCount.get(URL) + 1);
            } else {
                storeCount.put(URL, 1);
            }
            for (int i = 0; i < expectedDataValues.length; i++) {
                if (expectedDataValues[i][1].equals(URL) && expectedDataValues[i][2].equals(feature)
                        && expectedDataValues[i][3].equals(value)
                        && expectedDataValues[i][4].equals(timestamp)) {
                    storeFound.add((Integer) expectedDataValues[i][0]);
                }
            }
            rowCount++;
        }
        Assert.assertEquals(rowCount, 82);
        Assert.assertEquals(storeFound.size(), expectedDataValues.length);
        Assert.assertEquals(storeCount.get(expectedDataCount[0][0]), expectedDataCount[0][1]);
        Assert.assertEquals(storeCount.get(expectedDataCount[1][0]), expectedDataCount[1][1]);
    }
}
