package com.latticeengines.scoring.service.impl;

import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;
import com.latticeengines.scoring.service.ScoringJobService;

public class ScoringJobServiceImplUsingInterfaceIdTestNG extends ScoringFunctionalTestNGBase {

    @Autowired
    private ScoringJobService scoringJobService;

    private String tenant;

    private String path;

    private String dataPath;

    private String scorePath;

    private String uuid;
    
    private Map<String, Double> scores = new HashMap<>();

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        tenant = CustomerSpace.parse(this.getClass().getSimpleName()).toString();
        path = customerBaseDir + "/" + tenant;
        dataPath = customerBaseDir + "/" + tenant + "/data/Q_PLS_ModelingMulesoft_Relaunch/";
        HdfsUtils.mkdir(yarnConfiguration, dataPath);

        URL url1 = ClassLoader.getSystemResource("com/latticeengines/scoring/data/test.avro");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, url1.getFile(), dataPath);

        uuid = UUID.randomUUID().toString();
        URL modelSummaryUrl = ClassLoader
                .getSystemResource("com/latticeengines/scoring/models/RonModel1_2016-03-26_02-10_model.json");
        String modelPath = customerBaseDir + "/" + tenant + "/models/Q_PLS_ModelingMulesoft_Relaunch/" + uuid
                + "/1429553747321_0004";
        HdfsUtils.mkdir(yarnConfiguration, modelPath);
        String modelFilePath = modelPath + "/model.json";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), modelFilePath);

        scorePath = customerBaseDir + "/" + tenant + "/scoring/" + UUID.randomUUID() + "/scores";

        InputStream is = ClassLoader.getSystemResourceAsStream("com/latticeengines/scoring/results/scored.txt");
        List<String> lines = IOUtils.readLines(is);
        for (String line : lines) {
            String[] arr = line.split(",");
            scores.put(arr[0], Double.valueOf(arr[1]));
        }
    }

    @Test(groups = "functional")
    protected void score() throws Exception {
        ScoringConfiguration scoringConfig = new ScoringConfiguration();
        scoringConfig.setCustomer(tenant);
        scoringConfig.setSourceDataDir(dataPath);
        scoringConfig.setTargetResultDir(scorePath);
        scoringConfig.setModelGuids(Arrays.<String> asList(new String[] { "ms__" + uuid + "-PLS_model" }));
        scoringConfig.setUniqueKeyColumn(InterfaceName.Id.name());
        ApplicationId appId = scoringJobService.score(scoringConfig);
        waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);

        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, scorePath, ".*.avro");
        assertEquals(files.size(), 1);

        List<GenericRecord> records = AvroUtils.getData(yarnConfiguration, files);
        for (GenericRecord record : records) {
            assertNotNull(record.get(InterfaceName.Id.name()));
            assertNotNull(record.get(ScoreResultField.Percentile.displayName));
            assertNotNull(record.get(ScoreResultField.RawScore.name()));
            if (scores.containsKey(record.get(InterfaceName.Id.name()).toString())) {
                assertNotNull(record.get(ScoreResultField.Percentile.displayName));
                assertTrue(Math.abs(scores.get(record.get(InterfaceName.Id.name()).toString())
                        - ((Double) (record.get(ScoreResultField.RawScore.name())))) < 0.000001);
            }else{
                throw new Exception("missing id: " + record.get(InterfaceName.Id.name()));
            }

        }
    }

    @AfterMethod(enabled = true, lastTimeOnly = true, alwaysRun = true)
    public void afterEachTest() {
        try {
            HdfsUtils.rmdir(yarnConfiguration, path);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

}
