package com.latticeengines.scoring.exposed.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;

public class ScoringServiceImplTestNG extends ScoringFunctionalTestNGBase {

    @Autowired
    private ScoringServiceImpl scoringService;

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
