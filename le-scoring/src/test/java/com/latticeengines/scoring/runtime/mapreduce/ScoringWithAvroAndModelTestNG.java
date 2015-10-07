package com.latticeengines.scoring.runtime.mapreduce;

import java.sql.Timestamp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;
import com.latticeengines.scoring.service.ScoringStepYarnProcessor;
import com.latticeengines.scoring.service.impl.ScoringStepYarnProcessorImplTestNG;

public class ScoringWithAvroAndModelTestNG extends ScoringFunctionalTestNGBase {

    private static final double EPS = 1e-6;

    private static final Log log = LogFactory.getLog(ScoringStepYarnProcessorImplTestNG.class);

    @Autowired
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @Autowired
    private ScoringStepYarnProcessor scoringStepYarnProcessor;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Autowired
    private Configuration yarnConfiguration;

    private static final String customer = "Nutanix_TEST_DELL";

    private static String tenant;

    @Value("${scoring.test.table}")
    private String testInputTable;

    @Autowired
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    @Autowired
    private DbCreds scoringCreds;

    @Autowired
    private JdbcTemplate scoringJdbcTemplate;

    @Autowired
    private MetadataService metadataService;

    private String inputLeadsTable;

    private String modelPath;

    private String path;

    private String scorePath;

    @BeforeMethod(groups = "functional")
    public void beforeMethod() throws Exception {
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        inputLeadsTable = getClass().getSimpleName() + "__LeadsTable";
        tenant = CustomerSpace.parse(customer).toString();

        path = customerBaseDir + "/" + tenant + "/scoring/" + inputLeadsTable + "/data";
        scorePath = customerBaseDir + "/" + tenant + "/scoring/" + inputLeadsTable + "/scores";
    }

    @Test(groups = "functional", enabled = false)
    public void loadAndScore() throws Exception {
        ScoringCommand scoringCommand = new ScoringCommand(customer, ScoringCommandStatus.POPULATED, inputLeadsTable,
                0, 7727482, new Timestamp(System.currentTimeMillis()));
        // set a fake Pid
        scoringCommand.setPid(1234L);
        // submit scoring job directly without going through database
        scoringStepYarnProcessor.executeYarnStep(scoringCommand, ScoringCommandStep.SCORE_DATA);
    }

}
