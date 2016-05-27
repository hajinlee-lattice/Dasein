package com.latticeengines.scoring.exposed.service.impl;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.dmg.pmml.IOUtil;
import org.dmg.pmml.PMML;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.xml.sax.InputSource;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;
import com.latticeengines.scoring.exposed.domain.ScoringRequest;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;

public class ScoringServiceImplTestNG extends ScoringFunctionalTestNGBase {

    private PMML pmml;
    private List<ScoringRequest> requests;

    @Autowired
    private ScoringServiceImpl scoringService;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        InputStream pmmlInputStream = ClassLoader
                .getSystemResourceAsStream("com/latticeengines/scoring/LogisticRegressionPMML.xml");
        pmml = IOUtil.unmarshal(new InputSource(pmmlInputStream));
        requests = createListRequest(150);
    }

    @Test(groups = "functional")
    public void scoreBatch() {
        long time1 = System.currentTimeMillis();
        scoringService.scoreBatch(requests, pmml);
        long time2 = System.currentTimeMillis() - time1;
        System.out.println("Batch scoring elapsed time = " + time2);
    }

    @Test(groups = "functional")
    public void score() {
        long time1 = System.currentTimeMillis();
        for (ScoringRequest request : requests) {
            scoringService.score(request, pmml);
        }
        long time2 = System.currentTimeMillis() - time1;
        System.out.println("Serial scoring elapsed time = " + time2);
    }

    @Test(groups = "functional")
    public void testSubmitScoringYarnContainerJob() {
        RTSBulkScoringConfiguration rtsBulkScoringConfig = new RTSBulkScoringConfiguration();
        String tenant = "testTenant";
        rtsBulkScoringConfig.setCustomerSpace(CustomerSpace.parse(tenant));
        List<String> modelGuids = new ArrayList<String>();
        modelGuids.add("modelGuid");
        rtsBulkScoringConfig.setModelGuids(modelGuids);
        Table metadataTable = new Table();
        rtsBulkScoringConfig.setMetadataTable(metadataTable);
        ApplicationId appId = scoringService.submitScoreWorkflow(rtsBulkScoringConfig);
        System.out.println(appId);
    }

}
