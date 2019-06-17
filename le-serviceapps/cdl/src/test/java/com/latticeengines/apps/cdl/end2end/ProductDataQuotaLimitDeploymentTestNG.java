package com.latticeengines.apps.cdl.end2end;

import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class ProductDataQuotaLimitDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ProductDataQuotaLimitDeploymentTestNG.class);

    @Inject
    private WorkflowProxy workflowProxy;
    @Inject
    private CDLProxy cdlProxy;
    @Inject
    private EntityProxy entityProxy;

    private static final String jsonFileName = "cg-tenant-registration-datalimit.json";

    @BeforeClass(groups = "end2end")
    public void setup() {
        setupEnd2EndTestEnvironmentByFile(jsonFileName);
    }

    @Test(groups = "end2end")
    public void testDataQuotaLimit() throws Exception{
        resumeCheckpoint(ProcessAccountDeploymentTestNG.CHECK_POINT);
        verifyNumAttrsInAccount();
        verifyDateTypeAttrs();
        new Thread(() -> {
            createTestSegment1();
            createTestSegment2();
        }).start();
        importData();
        processAnalyze();

    }

    private void importData() {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        importData(BusinessEntity.Product, "Product.csv", null, false, false);
    }

    protected void processAnalyze() {
        log.info("Start processing and analyzing ...");
        ApplicationId appId = cdlProxy.processAnalyze(mainTestTenant.getId(), null);
        processAnalyzeAppId = appId.toString();
        log.info("processAnalyzeAppId=" + processAnalyzeAppId);
        Job Job = waitForWorkflow(appId.toString(),
                false);
        Assert.assertTrue(Job.getErrorMsg().endsWith("The data you uploaded has exceeded the limit."));
    }

    private Job waitForWorkflow(String applicationId, boolean running) {
        int retryOnException = 4;
        Job job;
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
                if (job.getJobStatus() == JobStatus.FAILED) {
                    log.error(applicationId + " Failed with ErrorCode " + job.getErrorCode() + ". \n"
                            + job.getErrorMsg());
                }
                return job;
            }
            try {
                Thread.sleep(30000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void verifyNumAttrsInAccount() {
        String tableName = dataCollectionProxy.getTableName(mainCustomerSpace,
                BusinessEntity.Product.getServingStore());
        List<ColumnMetadata> cms = metadataProxy.getTableColumns(mainCustomerSpace, tableName);
        log.info("product size is :" + cms.size());
        org.testng.Assert.assertTrue(cms.size() < 20000, "Should not have more than 20000 product attributes");
    }

    private void verifyDateTypeAttrs() {
        FrontEndQuery query = new FrontEndQuery();
        query.setMainEntity(BusinessEntity.Account);
        Bucket bkt = Bucket.dateBkt(TimeFilter.ever());
        Restriction restriction = new BucketRestriction(BusinessEntity.Account, "user_Test_Date", bkt);
        query.setAccountRestriction(new FrontEndRestriction(restriction));
        Long count = entityProxy.getCount(mainCustomerSpace, query);
        Assert.assertEquals(count, ACCOUNT_1);

        bkt = Bucket.dateBkt(TimeFilter.isEmpty());
        restriction = new BucketRestriction(BusinessEntity.Account, "user_Test_Date", bkt);
        query.setAccountRestriction(new FrontEndRestriction(restriction));
        count = entityProxy.getCount(mainCustomerSpace, query);
        Assert.assertEquals(count.longValue(), 0);
        log.info("verify date done");
    }
}
