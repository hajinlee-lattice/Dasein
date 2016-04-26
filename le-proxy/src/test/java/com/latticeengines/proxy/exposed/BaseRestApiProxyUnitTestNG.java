package com.latticeengines.proxy.exposed;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.ResourceAccessException;
import org.testng.annotations.Test;

import com.latticeengines.serviceruntime.exposed.exception.GetResponseErrorHandler;

@ContextConfiguration(locations = { "classpath:test-proxy-context.xml" })
public class BaseRestApiProxyUnitTestNG extends AbstractTestNGSpringContextTests {
    @Autowired
    private TestProxy testProxy;

    @Test(groups = "unit")
    public void testUrlExpansion() {
        testProxy.testUrlExpansion();
    }

    @Test(groups = "unit")
    public void testRetry() {
        boolean thrown = false;
        try {
            testProxy.testRetry();
        } catch (Exception e) {
            assertTrue(e instanceof ResourceAccessException);
            thrown = true;
        }
        assertTrue(thrown);
    }

    @Test(groups = "unit")
    public void testParseStackTrace() {
        String stackTrace = "com.latticeengines.domain.exposed.exception.RemoteLedpException: LEDP_00002: Generic system error.\n"
                + "\tat com.latticeengines.serviceruntime.exposed.exception.GetResponseErrorHandler.interpretAndThrowException(GetResponseErrorHandler.java:51)\n"
                + "\tat com.latticeengines.serviceruntime.exposed.exception.GetResponseErrorHandler.handleError(GetResponseErrorHandler.java:34)\n"
                + "\tat org.springframework.web.client.RestTemplate.handleResponseError(RestTemplate.java:576)\n"
                + "\tat org.springframework.web.client.RestTemplate.doExecute(RestTemplate.java:532)\n"
                + "\tat org.springframework.web.client.RestTemplate.execute(RestTemplate.java:489)\n"
                + "\tat org.springframework.web.client.RestTemplate.postForObject(RestTemplate.java:318)\n"
                + "\tat com.latticeengines.proxy.exposed.BaseRestApiProxy.post(BaseRestApiProxy.java:46)\n"
                + "\tat com.latticeengines.proxy.exposed.eai.EaiProxy.createExportDataJob(EaiProxy.java:27)\n"
                + "\tat com.latticeengines.serviceflows.workflow.export.ExportData.exportData(ExportData.java:41)\n"
                + "\tat com.latticeengines.serviceflows.workflow.export.ExportData.execute(ExportData.java:36)\n"
                + "\tat com.latticeengines.workflow.core.WorkflowTranslator$1.execute(WorkflowTranslator.java:100)\n"
                + "\tat org.springframework.batch.core.step.tasklet.TaskletStep$ChunkTransactionCallback.doInTransaction(TaskletStep.java:406)\n"
                + "\tat org.springframework.batch.core.step.tasklet.TaskletStep$ChunkTransactionCallback.doInTransaction(TaskletStep.java:330)\n"
                + "\tat org.springframework.transaction.support.TransactionTemplate.execute(TransactionTemplate.java:133)\n"
                + "\tat org.springframework.batch.core.step.tasklet.TaskletStep$2.doInChunkContext(TaskletStep.java:271)\n"
                + "\tat org.springframework.batch.core.scope.context.StepContextRepeatCallback.doInIteration(StepContextRepeatCallback.java:77)\n"
                + "\tat org.springframework.batch.repeat.support.RepeatTemplate.getNextResult(RepeatTemplate.java:368)\n"
                + "\tat org.springframework.batch.repeat.support.RepeatTemplate.executeInternal(RepeatTemplate.java:215)\n"
                + "\tat org.springframework.batch.repeat.support.RepeatTemplate.iterate(RepeatTemplate.java:144)\n"
                + "\tat org.springframework.batch.core.step.tasklet.TaskletStep.doExecute(TaskletStep.java:257)\n"
                + "\tat org.springframework.batch.core.step.AbstractStep.execute(AbstractStep.java:198)\n"
                + "\tat org.springframework.batch.core.job.SimpleStepHandler.handleStep(SimpleStepHandler.java:148)\n"
                + "\tat org.springframework.batch.core.job.AbstractJob.handleStep(AbstractJob.java:386)\n"
                + "\tat org.springframework.batch.core.job.SimpleJob.doExecute(SimpleJob.java:135)\n"
                + "\tat org.springframework.batch.core.job.AbstractJob.execute(AbstractJob.java:304)\n"
                + "\tat org.springframework.batch.core.launch.support.SimpleJobLauncher$1.run(SimpleJobLauncher.java:135)\n"
                + "\tat java.lang.Thread.run(Thread.java:745)";

        Exception e = new GetResponseErrorHandler().generateRemoteException(stackTrace);
        StackTraceElement[] stackTraceElements = e.getStackTrace();
        assertEquals(stackTraceElements.length, 26);
    }
}
