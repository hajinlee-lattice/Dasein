package com.latticeengines.scoring.functionalframework;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.AfterClass;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scoring.exposed.domain.ScoringRequest;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-scoring-context.xml" })
public class ScoringFunctionalTestNGBase extends DataPlatformFunctionalTestNGBase {

    protected static final Log log = LogFactory.getLog(ScoringFunctionalTestNGBase.class);

    @Autowired
    private ScoringOrderedEntityMgrListForDbClean scoringOrderedEntityMgrListForDbClean;

    protected RestTemplate restTemplate = new RestTemplate();

    @AfterClass(groups = { "functional", "functional.scheduler" })
    public void clearTables() {
        if (!doClearDbTables()) {
            return;
        }
        try {
            for (BaseEntityMgr<?> entityMgr : scoringOrderedEntityMgrListForDbClean.entityMgrs()) {
                entityMgr.deleteAll();
            }
        } catch (Exception e) {
            log.warn("Could not clear tables for all entity managers.", e);
        }
    }

    protected List<ScoringRequest> createListRequest(int numElements) {
        List<ScoringRequest> requests = new ArrayList<ScoringRequest>();
        Random random = new Random();
        for (int i = 0; i < numElements; i++) {
            ScoringRequest request = new ScoringRequest();
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("age", 30.0 + random.nextDouble() * 10.0);
            params.put("salary", 65000 + random.nextDouble() * 10000.0);
            params.put("car_location", random.nextInt(2) == 0 ? "street" : "carpark");
            request.setArguments(params);
            requests.add(request);
        }
        return requests;
    }

    protected void waitForSuccess(ApplicationId appId, ScoringCommandStep step) throws Exception {
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        log.info(step + ": appId succeeded: " + appId.toString());
    }

}
