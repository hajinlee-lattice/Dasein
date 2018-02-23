package com.latticeengines.workflowapi.flows;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.serviceflows.cdl.PlayLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiDeploymentTestNGBase;
import com.latticeengines.workflowapi.functionalframework.testDao.TestPlayDao;
import com.latticeengines.workflowapi.functionalframework.testDao.TestPlayLaunchDao;

public class PlayLaunchWorkflowDeploymentTestNG extends WorkflowApiDeploymentTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PlayLaunchWorkflowDeploymentTestNG.class);

    @SuppressWarnings("unused")
    private WorkflowExecutionId workflowId;

    @Autowired
    private TestPlayDao testPlayDao;

    @Autowired
    private TestPlayLaunchDao testPlayLaunchDao;

    private Play play;
    private PlayLaunch playLaunch;
    private MetadataSegment segment;

    @BeforeClass(groups = "workflow")
    public void setup() throws Exception {
        // setupTestTenant();
        // segment = createTestSegment();
        // play = createTestPlay();
        // playLaunch = createTestPlayLaunch(play);
    }

    @Test(groups = "workflow")
    public void testWorkflow() throws Exception {
        // PlayLaunchWorkflowConfiguration configuration =
        // generatePlayLaunchWorkflowConfiguration();
        // workflowService.registerJob(configuration.getWorkflowName(),
        // applicationContext);
        // workflowId = workflowService.start(configuration);
    }

    // @Test(groups = "workflow", dependsOnMethods = { "testWorkflow" },
    // expectedExceptions = AssertionError.class)
    public void testWorkflowStatus() throws Exception {
        // waitForCompletion(workflowId);
    }

    @AfterClass(groups = "workflow")
    public void cleanup() {
        // deleteTestPlay();
        // deleteTestPlayLaunch();
        // deleteTestSegment();
    }

    @SuppressWarnings("unused")
    private PlayLaunchWorkflowConfiguration generatePlayLaunchWorkflowConfiguration() throws Exception {
        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "playLaunchWorkflow");
        return new PlayLaunchWorkflowConfiguration.Builder() //
                .customer(mainTestCustomerSpace) //
                .workflow("playLaunchWorkflow") //
                .inputProperties(inputProperties) //
                .playLaunch(playLaunch) //
                .build();
    }

    @SuppressWarnings("unused")
    private PlayLaunch createTestPlayLaunch(Play play) {
        PlayLaunch playLaunch = new PlayLaunch();
        MetadataSegment segment = new MetadataSegment();
        segment.setDisplayName("TestSegment");
        playLaunch.setLaunchId("WorkFlowTestPlayLaunch");
        playLaunch.setPlay(play);
        playLaunch.setCreated(new Date());
        playLaunch.setTenantId(mainTestTenant.getPid());
        playLaunch.setTenant(mainTestTenant);
        playLaunch.setUpdated(new Date());
        playLaunch.setLaunchState(LaunchState.Launching);

        PlatformTransactionManager ptm = applicationContext.getBean("transactionManager",
                PlatformTransactionManager.class);
        TransactionTemplate tx = new TransactionTemplate(ptm);
        tx.execute(new TransactionCallbackWithoutResult() {
            public void doInTransactionWithoutResult(TransactionStatus status) {
                testPlayLaunchDao.create(playLaunch);
            }
        });
        return playLaunch;
    }

    @SuppressWarnings("unused")
    private Play createTestPlay() {
        Play play = new Play();
        play.setDisplayName("WorkFlowTestPlay");
        play.setCreatedBy("iamatest");
        play.setTenantId(mainTestTenant.getPid());
        play.setTenant(mainTestTenant);
        play.setUpdated(new Date());
        play.setCreated(new Date());
        play.setName(play.generateNameStr());
        PlatformTransactionManager ptm = applicationContext.getBean("transactionManager",
                PlatformTransactionManager.class);
        TransactionTemplate tx = new TransactionTemplate(ptm);
        tx.execute(new TransactionCallbackWithoutResult() {
            public void doInTransactionWithoutResult(TransactionStatus status) {
                testPlayDao.create(play);
            }
        });
        return play;
    }

    @SuppressWarnings("unused")
    private MetadataSegment createTestSegment() {
        MetadataSegment segment = new MetadataSegment();
        segment.setName("PlayLaunchWorkflowTestSegment");
        segment.setDisplayName("TestSegment");
        segment.setAccountRestriction(
                Restriction.builder().let(BusinessEntity.Account, "BUSINESS_NAME").isNull().build());

        restTemplate.postForObject(getPLSRestAPIHostPort() + "/pls/datacollection/segments", segment,
                MetadataSegment.class);

        MetadataSegment retrieved = restTemplate.getForObject(
                String.format(getPLSRestAPIHostPort() + "/pls/datacollection/segments/%s", segment.getName()),
                MetadataSegment.class);
        return retrieved;
    }

    @SuppressWarnings("unused")
    private void deleteTestPlay() {
        PlatformTransactionManager ptm = applicationContext.getBean("transactionManager",
                PlatformTransactionManager.class);
        TransactionTemplate tx = new TransactionTemplate(ptm);
        tx.execute(new TransactionCallbackWithoutResult() {
            public void doInTransactionWithoutResult(TransactionStatus status) {
                testPlayDao.delete(play);
            }
        });
    }

    @SuppressWarnings("unused")
    private void deleteTestPlayLaunch() {
        PlatformTransactionManager ptm = applicationContext.getBean("transactionManager",
                PlatformTransactionManager.class);
        TransactionTemplate tx = new TransactionTemplate(ptm);
        tx.execute(new TransactionCallbackWithoutResult() {
            public void doInTransactionWithoutResult(TransactionStatus status) {
                testPlayLaunchDao.delete(playLaunch);
            }
        });
    }

    @SuppressWarnings("unused")
    private void deleteTestSegment() {
        restTemplate
                .delete(String.format(getPLSRestAPIHostPort() + "/pls/datacollection/segments/%s", segment.getName()));
    }

}
