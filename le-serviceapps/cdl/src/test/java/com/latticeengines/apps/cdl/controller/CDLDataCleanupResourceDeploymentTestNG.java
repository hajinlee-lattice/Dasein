package com.latticeengines.apps.cdl.controller;

import static org.testng.Assert.assertNull;

import java.io.IOException;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.end2end.CDLEnd2EndDeploymentTestNGBase;
import com.latticeengines.apps.cdl.end2end.ProcessTransactionDeploymentTestNG;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftPartitionService;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

public class CDLDataCleanupResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CDLDataCleanupResourceDeploymentTestNG.class);

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private RedshiftPartitionService redshiftPartitionService;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = { "deployment" }, enabled = false)
    public void testCleanup() {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        DataFeed dataFeed = dataFeedProxy.getDataFeed(customerSpace.toString());
        dataFeedProxy.updateDataFeedStatus(customerSpace.toString(), DataFeed.Status.Active.getName());

        ApplicationId appId = cdlProxy.cleanupAll(customerSpace.toString(), null, MultiTenantContext.getEmailAddress());

        JobStatus status = waitForWorkflowStatus(appId.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);
    }

    @Test(groups = { "deployment" })
    public void testTenantCleanup() throws IOException {
        checkpointService.resumeCheckpoint( //
                ProcessTransactionDeploymentTestNG.CHECK_POINT, //
                CDLEnd2EndDeploymentTestNGBase.S3_CHECKPOINTS_VERSION);
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        log.info("cleaning up all data of " + customerSpace + " ... ");
        String tableA = dataCollectionProxy.getTable(customerSpace, BusinessEntity.Account.getServingStore()).getName();
        String tableB = dataCollectionProxy.getTable(customerSpace, BusinessEntity.Contact.getServingStore()).getName();
        log.info("cleaning up meta tables " + tableA + " ... " + tableB);
        RedshiftService redshiftService = redshiftPartitionService.getBatchUserService(null);
        List<String> redshiftTables = redshiftService.getTables(tableA);
        log.info("cleaning up redshift data of " + redshiftTables.toString() + " ... ");

        String tableC = dataCollectionProxy.getTable(customerSpace, BusinessEntity.Account.getBatchStore()).getName();
        log.info("batching store meta tables " + tableC + " ... ");

        cdlProxy.cleanupTenant(customerSpace);

        assertNull(dataCollectionProxy.getTable(customerSpace, BusinessEntity.Account.getServingStore()));
        assertNull(dataCollectionProxy.getTable(customerSpace, BusinessEntity.Contact.getServingStore()));

        redshiftTables = redshiftService.getTables(tableA);
        Assert.assertTrue(CollectionUtils.isEmpty(redshiftTables),
                String.format("Table %s is still in redshift", tableA));
        redshiftTables = redshiftService.getTables(tableB);
        Assert.assertTrue(CollectionUtils.isEmpty(redshiftTables),
                String.format("Table %s is still in redshift", tableB));

        redshiftTables = redshiftService.getTables(customerSpace);
        log.info("redshift tables after clean:" + redshiftTables.toString() + " ... ");
    }
}
