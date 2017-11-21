package com.latticeengines.apps.cdl.testframework;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Listeners;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.ProtectedRestApiProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-serviceapps-cdl-context.xml" })
public abstract class CDLDeploymentTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(CDLDeploymentTestNGBase.class);

    protected static final String SEGMENT_NAME = "CDLDeploymentTestSegment";

    @Resource(name = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed testBed;

    @Inject
    private WorkflowProxy workflowProxy;

    protected Tenant mainTestTenant;

    @Value("${common.test.pls.url}")
    protected String deployedHostPort;

    protected void setupTestEnvironment() throws NoSuchAlgorithmException, KeyManagementException, IOException {
        testBed.bootstrapForProduct(LatticeProduct.CG);
        mainTestTenant = testBed.getMainTestTenant();
        MultiTenantContext.setTenant(mainTestTenant);
        testBed.switchToSuperAdmin();
    }

    protected void attachProtectedProxy(ProtectedRestApiProxy proxy) {
        proxy.attachInterceptor(testBed.getPlsAuthInterceptor());
        logger.info("Attached the proxy " + proxy.getClass().getSimpleName() + " to GA testbed.");
    }

    protected MetadataSegment constructSegment(String segmentName) {
        MetadataSegment segment = new MetadataSegment();
        Restriction accountRestriction = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "LDC_Name"),
                Bucket.notNullBkt());
        segment.setAccountRestriction(accountRestriction);
        Bucket titleBkt = Bucket.valueBkt("Buyer");
        Restriction contactRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.Title.name()), titleBkt);
        segment.setContactRestriction(contactRestriction);
        segment.setDisplayName(segmentName);
        return segment;
    }

    protected RuleBasedModel constructRuleModel() {
        RatingRule ratingRule = new RatingRule();
        ratingRule.setDefaultBucketName(RuleBucketName.D.getName());

        Bucket bktA = Bucket.valueBkt(ComparisonType.IN_COLLECTION, //
                Arrays.asList("Mountain View", "New York", "Chicago", "Atlanta"));
        Restriction resA = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "LDC_City"), bktA);
        ratingRule.setRuleForBucket(RuleBucketName.A, resA, null);

        Bucket bktF = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("JOHN"));
        Restriction resF = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.ContactName.name()), bktF);
        ratingRule.setRuleForBucket(RuleBucketName.F, null, resF);

        RuleBasedModel ruleBasedModel = new RuleBasedModel();
        ruleBasedModel.setRatingRule(ratingRule);
        return ruleBasedModel;
    }

    protected JobStatus waitForWorkflowStatus(String applicationId, boolean running) {
        int retryOnException = 4;
        Job job = null;
        while (true) {
            try {
                job = workflowProxy.getWorkflowJobFromApplicationId(applicationId);
            } catch (Exception e) {
                log.error(String.format("Workflow job exception: %s", e.getMessage()), e);

                job = null;
                if (--retryOnException == 0)
                    throw new RuntimeException(e);
            }

            if ((job != null) && ((running && job.isRunning()) || (!running && !job.isRunning()))) {
                return job.getJobStatus();
            }
            try {
                Thread.sleep(30000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
