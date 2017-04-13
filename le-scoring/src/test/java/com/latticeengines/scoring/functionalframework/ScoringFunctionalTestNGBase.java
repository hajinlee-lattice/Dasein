package com.latticeengines.scoring.functionalframework;

import static org.testng.Assert.assertEquals;

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

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.db.exposed.service.DbMetadataService;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-scoring-context.xml" })
public class ScoringFunctionalTestNGBase extends DataPlatformFunctionalTestNGBase {

    protected static final Log log = LogFactory.getLog(ScoringFunctionalTestNGBase.class);

    @Autowired
    private ScoringOrderedEntityMgrListForDbClean scoringOrderedEntityMgrListForDbClean;
    
    @Autowired
    protected DbMetadataService dbMetadataService;

    protected RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

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
            log.warn("Could not clear tables for all entity managers.");
        }
    }

    protected void waitForSuccess(ApplicationId appId, ScoringCommandStep step) throws Exception {
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        log.info(step + ": appId succeeded: " + appId.toString());
    }

}
