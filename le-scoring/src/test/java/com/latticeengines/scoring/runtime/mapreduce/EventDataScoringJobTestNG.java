package com.latticeengines.scoring.runtime.mapreduce;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration;
import com.latticeengines.scoring.service.ScoringJobService;
import com.latticeengines.scoring.service.impl.ScoringJobServiceImpl;
import com.latticeengines.yarn.functionalframework.YarnMiniClusterFunctionalTestNGBase;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-scoring-context.xml" })
public class EventDataScoringJobTestNG extends YarnMiniClusterFunctionalTestNGBase {

    @Autowired
    private ScoringJobService scoringJobService;

    private String tenant;

    private String dataPath;

    private String scorePath;

    private String uuid;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        super.setup();
        tenant = CustomerSpace.parse(this.getClass().getSimpleName()).toString();
        dataPath = customerBaseDir + "/" + tenant + "/data/Q_PLS_ModelingMulesoft_Relaunch/";
        HdfsUtils.mkdir(miniclusterConfiguration, dataPath);

        URL url1 = ClassLoader.getSystemResource("com/latticeengines/scoring/data/allTest-r-00001.avro");
        HdfsUtils.copyLocalToHdfs(miniclusterConfiguration, url1.getFile(), dataPath);

        uuid = UUID.randomUUID().toString();
        URL modelSummaryUrl = ClassLoader.getSystemResource(
                "com/latticeengines/scoring/models/sampleModel/Hootsuite-lead-20160907-1516_2016-09-07_17-13_model.json");
        String modelPath = customerBaseDir + "/" + tenant + "/models/Q_PLS_ModelingMulesoft_Relaunch/" + uuid
                + "/1429553747321_0004";
        HdfsUtils.mkdir(miniclusterConfiguration, modelPath);
        String modelFilePath = modelPath + "/model.json";
        HdfsUtils.copyLocalToHdfs(miniclusterConfiguration, modelSummaryUrl.getFile(), modelFilePath);
        URL scoreDeviationUrl = ClassLoader
                .getSystemResource("com/latticeengines/scoring/models/sampleModel/enhancements/scorederivation.json");
        String enhancementsDir = modelPath + "/enhancements/scorederivation.json";
        HdfsUtils.copyLocalToHdfs(miniclusterConfiguration, scoreDeviationUrl.getFile(), enhancementsDir);

        scorePath = customerBaseDir + "/" + tenant + "/scoring/" + UUID.randomUUID() + "/scores";
    }

    @Override
    protected void uploadArtifactsToHdfs() throws IOException {
        super.uploadArtifactsToHdfs();
        String scoringHdfsPath = String.format("/app/%s/scoring", versionManager.getCurrentVersionInStack(stackName))
                .toString();
        FileUtils.deleteDirectory(new File("scoring"));
        HdfsUtils.copyHdfsToLocal(yarnConfiguration, scoringHdfsPath, ".");
        HdfsUtils.copyFromLocalToHdfs(miniclusterConfiguration, "scoring", scoringHdfsPath);
    }

    @Test(groups = "functional")
    public void test() throws Exception {
        ScoringConfiguration scoringConfig = new ScoringConfiguration();
        scoringConfig.setCustomer(tenant);
        scoringConfig.setSourceDataDir(dataPath);
        scoringConfig.setTargetResultDir(scorePath);
        scoringConfig.setModelGuids(Arrays.<String> asList(new String[] { "ms__" + uuid + "-PLS_model" }));
        scoringConfig.setUniqueKeyColumn(InterfaceName.Id.name());

        ((ScoringJobServiceImpl) scoringJobService).setConfiguration(miniclusterConfiguration);
        Properties properties = ((ScoringJobServiceImpl) scoringJobService).generateCustomizedProperties(scoringConfig);
        testMRJob(EventDataScoringJob.class, properties);

    }
}
