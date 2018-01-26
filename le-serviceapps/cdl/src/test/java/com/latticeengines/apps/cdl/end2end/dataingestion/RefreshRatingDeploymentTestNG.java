package com.latticeengines.apps.cdl.end2end.dataingestion;

import java.util.Collections;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;

public class RefreshRatingDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RefreshRatingDeploymentTestNG.class);

    private RatingEngine rule1;
    private RatingEngine rule2;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);

        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));

        createTestSegment2();
        rule1 = createRuleBasedRatingEngine();
        rule2 = createRuleBasedRatingEngine();
        ratingEngineProxy.updateRatingEngineCounts(mainTestTenant.getId(), rule1.getId());
        ratingEngineProxy.updateRatingEngineCounts(mainTestTenant.getId(), rule2.getId());
        verifyRuleBasedEngines();

        processAnalyze(constructRequest());
        verifyProcess();
    }

    private void verifyProcess() {
        runCommonPAVerifications();
        verifyRuleBasedEngines();
    }

    private void verifyRuleBasedEngines() {
        Map<RuleBucketName, Long> ratingCounts = ImmutableMap.of( //
                RuleBucketName.A, RATING_A_COUNT_1, //
                RuleBucketName.D, RATING_D_COUNT_1, //
                RuleBucketName.F, RATING_F_COUNT_1
        );
        verifyRatingEngineCount(rule1.getId(), ratingCounts);
        verifyRatingEngineCount(rule2.getId(), ratingCounts);
    }

    private ProcessAnalyzeRequest constructRequest() {
        ProcessAnalyzeRequest request = new ProcessAnalyzeRequest();
        request.setRebuildEntities(Collections.singletonList(BusinessEntity.Rating));
        return request;
    }

}
