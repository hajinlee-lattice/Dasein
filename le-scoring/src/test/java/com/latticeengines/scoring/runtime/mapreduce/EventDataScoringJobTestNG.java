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
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration.ScoringInputType;
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

    private String uuid = "f7f1eb16-0d26-4aa1-8c4a-3ac696e13d06";

    @Override
    @BeforeClass(groups = "functional", enabled = false)
    public void setup() throws Exception {
        super.setup();
        tenant = CustomerSpace.parse(this.getClass().getSimpleName()).toString();
        dataPath = customerBaseDir + "/" + tenant + "/data/Q_PLS_ModelingMulesoft_Relaunch/";
        HdfsUtils.mkdir(miniclusterConfiguration, dataPath);

        URL url1 = ClassLoader.getSystemResource("com/latticeengines/scoring/data/s100Test-mulesoft-scoring.avro");
        HdfsUtils.copyLocalToHdfs(miniclusterConfiguration, url1.getFile(), dataPath);

        URL modelSummaryUrl = ClassLoader.getSystemResource(
                "com/latticeengines/scoring/models/sampleModel/Model_Submission1_2018-01-09_08-21_model.json");
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
        String scoringHdfsPath = String.format("%s/scoring", manifestService.getLedpPath());
        FileUtils.deleteDirectory(new File("scoring"));
        HdfsUtils.copyHdfsToLocal(yarnConfiguration, scoringHdfsPath, ".");
        HdfsUtils.copyFromLocalToHdfs(miniclusterConfiguration, "scoring", scoringHdfsPath);
    }

    @Test(groups = "functional", enabled = false)
    public void test() throws Exception {
        ScoringConfiguration scoringConfig = new ScoringConfiguration();
        scoringConfig.setCustomer(tenant);
        scoringConfig.setSourceDataDir(dataPath + "/s100Test-mulesoft-scoring.avro");
        scoringConfig.setTargetResultDir(scorePath);
        scoringConfig.setModelGuids(Arrays.<String> asList(new String[] { "ms__" + uuid + "-PLS_model" }));
        scoringConfig.setUniqueKeyColumn(InterfaceName.__Composite_Key__.name());
        scoringConfig.setScoreInputType(ScoringInputType.Avro);
        scoringConfig.setModelIdFromRecord(true);
        scoringConfig.setUseScorederivation(false);

        ((ScoringJobServiceImpl) scoringJobService).setConfiguration(miniclusterConfiguration);
        Properties properties = ((ScoringJobServiceImpl) scoringJobService).generateCustomizedProperties(scoringConfig);
        testMRJob(EventDataScoringJob.class, properties);

    }

    // public static void main(String[] args) throws IOException {
    // String uuid = "f7f1eb16-0d26-4aa1-8c4a-3ac696e13d06";
    // URL url1 =
    // ClassLoader.getSystemResource("com/latticeengines/scoring/data/s100Test-mulesoft.avro");
    //
    // DatumWriter userDatumWriter = new GenericDatumWriter<GenericRecord>();
    // DataFileWriter dataFileWriter = new DataFileWriter(userDatumWriter);
    //
    // Table t = MetadataConverter.getTable(new Configuration(),
    // url1.getPath());
    // Attribute a = new Attribute();
    // a.setName(ScoringDaemonService.MODEL_GUID);
    // a.setDisplayName(a.getName());
    // a.setPhysicalDataType("String");
    // t.addAttribute(a);
    //
    // t.getAttribute("ModelingID").setPhysicalDataType("String");
    // t.getAttribute("ModelingID").setName(InterfaceName.__Composite_Key__.name());
    //
    // Schema s = TableUtils.createSchema("scoringtable", t);
    // dataFileWriter.create(s, new File("s100Test-mulesoft-scoring.avro"));
    // List<GenericRecord> records =
    // AvroUtils.readFromLocalFile(url1.getPath());
    // GenericRecordBuilder builder = new GenericRecordBuilder(s);
    // for (GenericRecord r : records) {
    // builder.set(ScoringDaemonService.MODEL_GUID, "ms__" + uuid +
    // "-PLS_model");
    // for (Field f : s.getFields()) {
    // if (!f.name().equals(ScoringDaemonService.MODEL_GUID)) {
    // if (f.name().equals(InterfaceName.__Composite_Key__.name())) {
    // builder.set(f, r.get("ModelingID") + "");
    // } else {
    // builder.set(f, r.get(f.name()));
    // }
    // }
    // }
    // dataFileWriter.append(builder.build());
    // }
    // }
}
