package com.latticeengines.apps.cdl.workflow;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.BucketedAccount;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.end2end.CDLEnd2EndDeploymentTestNGBase;
import com.latticeengines.apps.cdl.end2end.ProcessAccountWithAdvancedMatchDeploymentTestNG;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.testframework.CDLWorkflowFrameworkDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.AtlasProfileReportWorkflowConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

/**
 * dpltc deploy -a admin,pls,lp,cdl,eai,metadata,matchapi,workflowapi,datacloudapi
 */
public class AtlasProfileReportWorkflowDeploymentTestNG extends CDLWorkflowFrameworkDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AtlasProfileReportWorkflowDeploymentTestNG.class);

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private StatisticsContainer container1;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironment();
        checkpointService.resumeCheckpoint( //
                ProcessAccountWithAdvancedMatchDeploymentTestNG.CHECK_POINT, //
                CDLEnd2EndDeploymentTestNGBase.S3_CHECKPOINTS_VERSION);
    }

    @Override
    @Test(groups = {"deployment"})
    public void testWorkflow() throws Exception {
        container1 = removeAttrsFromStats();
        AtlasProfileReportWorkflowConfiguration configuration = new AtlasProfileReportWorkflowConfiguration.Builder() //
                .customer(mainTestCustomerSpace) //
                .allowInternalEnrichAttrs(false) //
                .userId("test-user@lattice-engines.com") //
                .build();
        String workflowName = AtlasProfileReportWorkflowConfiguration.WORKFLOW_NAME;
        runWorkflow(generateWorkflowTestConfiguration(null, workflowName, configuration, null));
        verifyTest();
    }

    @Override
    protected void verifyTest() {
        StatisticsContainer container2 = dataCollectionService.getStats(mainCustomerSpace, null, null);
        Assert.assertNotNull(container2);
        log.info("Expanded the account cube to {} attributes.", //
                container2.getStatsCubes().get(Account.name()).getStatistics().size());
        compareContainers(container1, container2);
    }

    private StatisticsContainer removeAttrsFromStats() {
        StatisticsContainer container = dataCollectionProxy.getStats(mainCustomerSpace, null);
        Table accountTable = dataCollectionProxy.getTable(mainCustomerSpace, BucketedAccount);
        Set<String> accountAttrs = new HashSet<>(Arrays.asList(accountTable.getAttributeNames()));
        Map<String, StatsCube> statsCubeMap = container.getStatsCubes();
        StatsCube accountCube = statsCubeMap.get(Account.name());
        for (String attrName: new ArrayList<>(accountCube.getStatistics().keySet())) {
            if (!accountAttrs.contains(attrName)) {
                accountCube.getStatistics().remove(attrName);
            }
        }
        container.setStatsCubes(statsCubeMap);
        container.setName(null);
        int numAccountAttrs = container.getStatsCubes().get(Account.name()).getStatistics().size();
        log.info("Reduce account cube to {} attributes", numAccountAttrs);
        Assert.assertEquals(numAccountAttrs, accountAttrs.size());
        dataCollectionProxy.upsertStats(mainCustomerSpace, container);
        RetryTemplate retry = RetryUtils //
                .getRetryTemplate(10, Collections.singleton(AssertionError.class), null);
        return retry.execute(ctx -> {
            SleepUtils.sleep(1000L);
            StatisticsContainer ctr = dataCollectionProxy.getStats(mainCustomerSpace, null);
            Assert.assertEquals(ctr.getStatsCubes().get(Account.name()).getStatistics().size(), numAccountAttrs);
            return ctr;
        });
    }

    private void compareContainers(StatisticsContainer container1, StatisticsContainer container2) {
        Assert.assertEquals(container1.getStatsCubes().size(), container2.getStatsCubes().size());
        Map<String, AttributeStats> cube1 = container1.getStatsCubes().get(Account.name()).getStatistics();
        Map<String, AttributeStats> cube2 = container2.getStatsCubes().get(Account.name()).getStatistics();
        for (String attr1: cube1.keySet()) {
            Assert.assertTrue(cube2.containsKey(attr1), String.format("%s only exists in old cube", attr1));
        }
        Assert.assertTrue(cube2.size() > cube1.size(),
                String.format("The new cube [%d] is not more than the old cube [%d]", cube2.size(), cube1.size()));
    }

}
