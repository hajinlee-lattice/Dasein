package com.latticeengines.apps.cdl.end2end.dataingestion;


import static org.testng.Assert.assertEquals;

import java.util.Collections;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.RatingEngineModelingParameters;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.testframework.exposed.proxy.pls.ModelSummaryProxy;
import com.latticeengines.testframework.exposed.proxy.pls.PlsModelProxy;

/**
 * This test is for generating model artifacts for other tests
 */
public class CreateAIModelDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CreateAIModelDeploymentTestNG.class);
    private static final boolean USE_EXISTING_TENANT = true;
    private static final String EXISTING_TENANT = "LETest1517366010516";

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    private PlsModelProxy plsModelProxy;

    private RatingEngineModelingParameters modelingParameters;
    private final String prodId = "A80D4770376C1226C47617C071324C0B";

    @BeforeClass(groups = { "end2end" })
    public void setup() throws Exception {
        if (USE_EXISTING_TENANT) {
            testBed.useExistingTenantAsMain(EXISTING_TENANT);
            testBed.switchToSuperAdmin();
            mainTestTenant = testBed.getMainTestTenant();
        } else {
            super.setup();
            resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        }
    }

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        attachProtectedProxy(modelSummaryProxy);
        attachProtectedProxy(plsModelProxy);
        RatingEngineModelingParameters parameters = createModelingParameters();
        model(parameters);
    }

    private RatingEngineModelingParameters createModelingParameters() {
        modelingParameters = new RatingEngineModelingParameters();
        modelingParameters.setName(NamingUtils.timestamp("CDLEnd2End"));
        modelingParameters.setDisplayName("Create AI Engine Test Model");
        modelingParameters.setDescription("Test");
        modelingParameters.setModuleName("module");
        modelingParameters.setActivateModelSummaryByDefault(true);
        modelingParameters.setExpectedValue(true);
        modelingParameters.setLiftChart(true);

        Bucket.Transaction txn = new Bucket.Transaction(prodId, TimeFilter.ever(), null, null, false);
        EventFrontEndQuery query = getQuery(txn);
        modelingParameters.setTrainFilterQuery(query);
        modelingParameters.setEventFilterQuery(query);
        modelingParameters.setTargetFilterQuery(query);
        return modelingParameters;
    }

    private void model(ModelingParameters parameters) {
        log.info("Start modeling ...");
        System.out.println("json=" + JsonUtils.serialize(parameters));
        ApplicationId modelingWorkflowApplicationId = plsModelProxy.createRatingModel(parameters);
        log.info(String.format("Workflow application id is %s", modelingWorkflowApplicationId));
        JobStatus completedStatus = waitForWorkflowStatus(modelingWorkflowApplicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    private EventFrontEndQuery getQuery(Bucket.Transaction txn) {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Transaction, "AnyThing");
        EventFrontEndQuery frontEndQuery = new EventFrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        Bucket bucket = Bucket.txnBkt(txn);
        Restriction restriction = new BucketRestriction(attrLookup, bucket);
        frontEndRestriction.setRestriction(restriction);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setTargetProductIds(Collections.singletonList(prodId));
        return frontEndQuery;
    }

}
