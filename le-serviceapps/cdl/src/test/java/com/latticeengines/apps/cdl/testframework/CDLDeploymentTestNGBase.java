package com.latticeengines.apps.cdl.testframework;

import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Listeners;

import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.ProtectedRestApiProxy;
import com.latticeengines.proxy.exposed.dataplatform.ModelProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.testframework.exposed.proxy.pls.ModelSummaryProxy;
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

    @Value("${common.test.pls.url}")
    protected String internalResourceHostPort;

    protected InternalResourceRestApiProxy internalResourceProxy;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private ModelProxy modelProxy;

    @Inject
    private Configuration yarnConfiguration;

    protected Tenant mainTestTenant;

    @Value("${common.test.pls.url}")
    protected String deployedHostPort;

    @Value("${camille.zk.pod.id}")
    private String podId;

    protected void setupTestEnvironment() {
        testBed.bootstrapForProduct(LatticeProduct.CG);
        mainTestTenant = testBed.getMainTestTenant();
        MultiTenantContext.setTenant(mainTestTenant);
        testBed.switchToSuperAdmin();
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    protected void attachProtectedProxy(ProtectedRestApiProxy proxy) {
        testBed.attachProtectedProxy(proxy);
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
        ratingRule.setDefaultBucketName(RatingBucketName.D.getName());

        Bucket bktA = Bucket.valueBkt(ComparisonType.IN_COLLECTION, //
                Arrays.asList("Mountain View", "New York", "Chicago", "Atlanta"));
        Restriction resA = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "LDC_City"), bktA);
        ratingRule.setRuleForBucket(RatingBucketName.A, resA, null);

        Bucket bktF = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("JOHN"));
        Restriction resF = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.ContactName.name()), bktF);
        ratingRule.setRuleForBucket(RatingBucketName.F, null, resF);

        RuleBasedModel ruleBasedModel = new RuleBasedModel();
        ruleBasedModel.setRatingRule(ratingRule);
        return ruleBasedModel;
    }

    protected JobStatus waitForWorkflowStatus(String applicationId, boolean running) {
        int retryOnException = 4;
        Job job = null;
        while (true) {
            try {
                job = workflowProxy.getWorkflowJobFromApplicationId(applicationId,
                        CustomerSpace.parse(mainTestTenant.getId()).toString());
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

    public void setMainTestTenant(Tenant mainTestTenant) {
        this.mainTestTenant = mainTestTenant;
    }

    protected String uploadModel(String tarballResourceUrl) {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(tarballResourceUrl);
        String stagingDir = "models" + File.separator + UUID.randomUUID().toString();
        try {
            CompressionUtils.untarInputStream(is, stagingDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to untar model artifacts " + tarballResourceUrl, e);
        }

        String appId = new File(stagingDir).list()[0];

        String modelSummaryPath = stagingDir + File.separator + appId + File.separator + "enhancements" + File.separator
                + "modelsummary.json";
        String modelSummaryContent;
        try {
            modelSummaryContent = FileUtils.readFileToString(new File(modelSummaryPath), Charset.forName("UTF-8"));
        } catch (Exception e) {
            throw new RuntimeException("Failed to read model summary content at " + modelSummaryPath, e);
        }
        modelSummaryContent = modelSummaryContent.replaceAll("/Pods/Default/", "/Pods/" + podId + "/");

        Pattern pattern = Pattern.compile("/Contracts/(.*)/Tenants/");
        Matcher matcher = pattern.matcher(modelSummaryContent);
        String tenantName = "";
        if (matcher.find()) {
            tenantName = matcher.group(1);
            log.info("Found tenant name " + tenantName + " in json.");
        } else {
            Assert.fail("Cannot find tenant name from model summary json");
        }

        String newTenantName = CustomerSpace.parse(mainTestTenant.getId()).getTenantId();
        String newUuid = UUID.randomUUID().toString();

        String patternStr = String.format("%s.%s.Production\\|(.*)\\|([^\"]+)", tenantName, tenantName);
        pattern = Pattern.compile(patternStr);
        matcher = pattern.matcher(modelSummaryContent);
        String eventTable = "";
        String uuid = "";
        if (matcher.find()) {
            String lookupId = matcher.group(0);
            eventTable = matcher.group(1);
            uuid = matcher.group(2);
            String newLookupId = String.format("%s.%s.Production|%s|%s", newTenantName, newTenantName, eventTable, uuid);
            modelSummaryContent = modelSummaryContent.replace(lookupId, newLookupId);
            modelSummaryContent = modelSummaryContent.replace(uuid, newUuid);
        } else {
            Assert.fail("Cannot find and parse lookup id from model summary json.");
        }

        String hdfsPathParttern = "/user/s-analytics/customers/%s.%s.Production/models/%s/%s/%s";
        String hdfsPath = String.format(hdfsPathParttern, newTenantName, newTenantName, eventTable, newUuid, appId);

        modelSummaryContent = modelSummaryContent.replace(tenantName, newTenantName);
        try {
            FileUtils.write(new File(modelSummaryPath), modelSummaryContent, Charset.forName("UTF-8"));
        } catch (Exception e) {
            throw new RuntimeException("Failed to write modified model summary.", e);
        }

        try {
            HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, stagingDir + File.separator + appId, hdfsPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to upload model artifacts to " + hdfsPath, e);
        }

        modelProxy.flagToDownload(mainTestTenant.getId());
        FileUtils.deleteQuietly(new File(stagingDir));

        return newUuid;
    }

    protected ModelSummary waitToDownloadModelSummaryWithUuid(ModelSummaryProxy modelSummaryProxy, String uuid)
            throws InterruptedException {
        ModelSummary found = null;
        while (true) {
            log.info(String.format("Getting the model whose id contains %s", uuid));
            List<ModelSummary> summaries = modelSummaryProxy.getSummaries();
            if (CollectionUtils.isNotEmpty(summaries)) {
                for (ModelSummary summary: summaries) {
                    if (summary.getId().contains(uuid)) {
                        found = summary;
                        break;
                    }
                }
            }
            if (found != null)
                break;
            Thread.sleep(10000);
        }
        assertNotNull(found);

        // Look up the model summary with details
        return modelSummaryProxy.getModelSummary(found.getId());
    }

}
