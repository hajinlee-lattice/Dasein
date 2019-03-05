package com.latticeengines.scoring.functionalframework;

import static org.testng.Assert.assertEquals;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.testng.annotations.AfterClass;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.db.exposed.service.DbMetadataService;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.yarn.functionalframework.YarnFunctionalTestNGBase;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-scoring-context.xml" })
public class ScoringFunctionalTestNGBase extends YarnFunctionalTestNGBase {

    protected static final Logger log = LoggerFactory.getLogger(ScoringFunctionalTestNGBase.class);

    @Inject
    private ScoringOrderedEntityMgrListForDbClean scoringOrderedEntityMgrListForDbClean;

    @Inject
    protected DbMetadataService dbMetadataService;

    @Value("${dataplatform.customer.basedir}")
    protected String customerBaseDir;

    private boolean doClearDbTables() {
        return true;
    }

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
