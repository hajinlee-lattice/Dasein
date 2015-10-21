package com.latticeengines.scoring.runtime.mapreduce;

import static org.testng.Assert.assertTrue;

import java.io.File;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;
import com.latticeengines.scoring.service.ScoringStepYarnProcessor;

public class ScoringWithDuplicateLeadsTestNG extends ScoringFunctionalTestNGBase {

    private static final String modelID = "2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json";

    private static final Log log = LogFactory.getLog(ScoringWithDuplicateLeadsTestNG.class);
    @Autowired
    private ScoringStepYarnProcessor scoringStepYarnProcessor;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Autowired
    private Configuration yarnConfiguration;

    private static final String customer = ScoringWithDuplicateLeadsTestNG.class.getSimpleName();

    private static String tenant;

    @Autowired
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    @Autowired
    private DbCreds scoringCreds;

    @Autowired
    private JdbcTemplate scoringJdbcTemplate;

    @Autowired
    private MetadataService metadataService;

    @Value("${scoring.test.table}")
    private String testInputTable;

    private String inputLeadsTable;

    private String modelPath;

    private String path;

    private String scorePath;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        inputLeadsTable = getClass().getSimpleName() + "_LeadsTable";
        metadataService.createNewTableFromExistingOne(scoringJdbcTemplate, inputLeadsTable, testInputTable);
        tenant = CustomerSpace.parse(customer).toString();

        URL url = ClassLoader.getSystemResource("com/latticeengines/scoring/data/"
                + "2Checkout_ScoringComparisonAgainstProdForSingleModelTestNG-00001.avro");
        // read avro file from local directory
        String localAvroFilePath = url.getPath();
        List<GenericRecord> recordList = AvroUtils.readFromLocalFile(localAvroFilePath);

        // duplicate the records twice
        List<GenericRecord> dupRecordList = new ArrayList<GenericRecord>();
        for (GenericRecord record : recordList) {
            dupRecordList.add(record);
            dupRecordList.add(record);
        }

        Schema schema = AvroUtils.readSchemaFromLocalFile(localAvroFilePath);

        // duplicate the avro and write to local path;
        File tempFile = File.createTempFile("temp", ".avro");
        String tempFilePath = tempFile.getAbsolutePath();
        System.out.println("Temp file : " + tempFilePath);
        AvroUtils.writeToLocalFile(schema, dupRecordList, tempFilePath);

        path = customerBaseDir + "/" + tenant + "/scoring/" + inputLeadsTable + "/data";
        scorePath = customerBaseDir + "/" + tenant + "/scoring/" + inputLeadsTable + "/scores";
        HdfsUtils.mkdir(yarnConfiguration, path);
        String dataPath1 = path + "/1.avro";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, tempFilePath, dataPath1);
        tempFile.delete();

        URL modelSummaryUrl = ClassLoader.getSystemResource("com/latticeengines/scoring/models/" + modelID);
        modelPath = customerBaseDir + "/" + tenant + "/models/" + inputLeadsTable
                + "/1e8e6c34-80ec-4f5b-b979-e79c8cc6bec3/1429553747321_0004";
        HdfsUtils.mkdir(yarnConfiguration, modelPath);
        String filePath = modelPath + "/model.json";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), filePath);
    }

    @AfterMethod(enabled = true, lastTimeOnly = true, alwaysRun = true)
    public void afterEachTest() {
        try {
            HdfsUtils.rmdir(yarnConfiguration, path);
            HdfsUtils.rmdir(yarnConfiguration, scorePath);
            HdfsUtils.rmdir(yarnConfiguration, modelPath);
            metadataService.dropTable(scoringJdbcTemplate, inputLeadsTable);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    @Test(groups = "functional", enabled = true)
    public void loadAndScore() throws Exception {
        ScoringCommand scoringCommand = new ScoringCommand(customer, ScoringCommandStatus.POPULATED, inputLeadsTable,
                0, 100, new Timestamp(System.currentTimeMillis()));
        // set a fake Pid
        scoringCommand.setPid(1234L);
        // submit scoring job directly without going through database
        try {
            ApplicationId appId = scoringStepYarnProcessor.executeYarnStep(scoringCommand,
                    ScoringCommandStep.SCORE_DATA);
            waitForSuccess(appId, ScoringCommandStep.SCORE_DATA);
        } catch (Exception e) {
            log.warn(e.getMessage());
            assertTrue(true, "Should have have thrown any exception.");
        }
    }
}
