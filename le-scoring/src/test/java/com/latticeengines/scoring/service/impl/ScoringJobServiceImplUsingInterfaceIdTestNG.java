package com.latticeengines.scoring.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

    @BeforeClass(groups = "functional", enabled = false)
    public void setup() throws Exception {
        tenant = CustomerSpace.parse(this.getClass().getSimpleName()).toString();
        path = customerBaseDir + "/" + tenant;
        dataPath = customerBaseDir + "/" + tenant + "/data/Q_PLS_ModelingMulesoft_Relaunch/";
        HdfsUtils.mkdir(yarnConfiguration, dataPath);

        URL url1 = ClassLoader.getSystemResource("com/latticeengines/scoring/data/allTest-r-00001.avro");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, url1.getFile(), dataPath);

        uuid = UUID.randomUUID().toString();
        URL modelSummaryUrl = ClassLoader
                .getSystemResource("com/latticeengines/scoring/models/sampleModel/Hootsuite-lead-20160907-1516_2016-09-07_17-13_model.json");
        String modelPath = customerBaseDir + "/" + tenant + "/models/Q_PLS_ModelingMulesoft_Relaunch/" + uuid
                + "/1429553747321_0004";
        HdfsUtils.mkdir(yarnConfiguration, modelPath);
        String modelFilePath = modelPath + "/model.json";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), modelFilePath);
        URL scoreDeviationUrl = ClassLoader
                .getSystemResource("com/latticeengines/scoring/models/sampleModel/enhancements/scorederivation.json");
        String enhancementsDir = modelPath + "/enhancements/scorederivation.json";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, scoreDeviationUrl.getFile(), enhancementsDir);

        scorePath = customerBaseDir + "/" + tenant + "/scoring/" + UUID.randomUUID() + "/scores";
        InputStream is = ClassLoader
                .getSystemResourceAsStream("com/latticeengines/scoring/models/sampleModel/Hootsuite-lead-20160907-1516_2016-09-07_17-13_scored.txt");
        List<String> lines = IOUtils.readLines(is, "UTF-8");
        for (String line : lines) {
            String[] arr = line.split(",");
            scores.put(arr[0], Double.valueOf(arr[2]));
        }
    }

    @Test(groups = "functional", enabled = false)
    protected void score() throws Exception {
        ScoringConfiguration scoringConfig = new ScoringConfiguration();
        scoringConfig.setCustomer(tenant);
        scoringConfig.setSourceDataDir(dataPath);
        scoringConfig.setTargetResultDir(scorePath);
        scoringConfig.setModelGuids(Collections.singletonList("ms__" + uuid + "-PLS_model"));
        scoringConfig.setUniqueKeyColumn(InterfaceName.InternalId.name());
        scoringConfig.setModelIdFromRecord(false);
        ApplicationId appId = scoringJobService.score(scoringConfig);
        waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);

        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, scorePath, ".*.avro");
        assertEquals(files.size(), 1);

        List<GenericRecord> records = AvroUtils.getData(yarnConfiguration, files);
        for (GenericRecord record : records) {
            assertNotNull(record.get(InterfaceName.InternalId.name()));
            assertNotNull(record.get(ScoreResultField.Percentile.displayName));
            assertNotNull(record.get(ScoreResultField.RawScore.name()));
            if (scores.containsKey(record.get(InterfaceName.InternalId.name()).toString())) {
                assertNotNull(record.get(ScoreResultField.Percentile.displayName));
                assertTrue(Math.abs(scores.get(record.get(InterfaceName.InternalId.name()).toString())
                        - ((Double) (record.get(ScoreResultField.RawScore.name())))) < 0.000001);
            } else {
                throw new Exception("missing id: " + record.get(InterfaceName.InternalId.name()));
            }

        }
    }

    @Override
    @AfterMethod(lastTimeOnly = true, alwaysRun = true, enabled = false)
    public void afterEachTest() {
        try {
            HdfsUtils.rmdir(yarnConfiguration, path);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

}
