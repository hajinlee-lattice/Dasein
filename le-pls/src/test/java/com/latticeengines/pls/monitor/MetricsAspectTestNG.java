package com.latticeengines.pls.monitor;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.monitor.metrics.MetricsAspect;
import com.latticeengines.pls.controller.ModelSummaryResource;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;

public class MetricsAspectTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private ModelSummaryResource modelSummaryResource;

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    private Log origLog;
    private ModelSummaryEntityMgr origSummaryEntityMgr;

    @BeforeClass
    public void beforeClass() {
        origLog = MetricsAspect.log;
        origSummaryEntityMgr = modelSummaryResource.getModelSummaryEntityMgr();
    }

    @AfterClass
    public void afterClass() {
        MetricsAspect.log = origLog;
        modelSummaryResource.setModelSummaryEntityMgr(origSummaryEntityMgr);
    }

    @Test(groups = { "functional" })
    public void logMetrics() throws Exception {
        Log newLog = mock(Log.class);
        MetricsAspect.log = newLog;

        origSummaryEntityMgr = modelSummaryResource.getModelSummaryEntityMgr();
        final List<String> logs = new ArrayList<>();
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] params = invocation.getArguments();
                logs.add((String) params[0]);
                return logs;
            }
        }).when(newLog).info(any());

        ModelSummaryEntityMgr summaryEntityMgr = mock(ModelSummaryEntityMgr.class);
        modelSummaryResource.setModelSummaryEntityMgr(summaryEntityMgr);

        modelSummaryResource.delete("1");
        verify(newLog, times(1)).info(anyString());
        Assert.assertTrue(logs.get(0).contains("Metrics for API=ModelSummaryResource.delete(..) ElapsedTime="));
        Assert.assertTrue(logs.get(0).contains("Track Id="));

        modelSummaryResource.getModelSummaries(null);
        verify(newLog, times(2)).info(anyString());

        modelSummaryResource.getModelSummaryEntityMgr();
        verify(newLog, times(2)).info(anyString());

        String passwd = DigestUtils.sha256Hex(adminPassword);
        Ticket ticket = globalAuthenticationService.authenticateUser(adminUsername, passwd);
        assertNotNull(ticket);
        assertTrue(ticket.getTenants().size() >= 2);
        boolean result = globalAuthenticationService.discard(ticket);
        assertTrue(result);
        Assert.assertTrue(logs.get(2).contains(
                "Metrics for API=GlobalAuthenticationService.authenticateUser(..) ElapsedTime="));
        Assert.assertTrue(logs.get(2).contains("Track Id="));
        Assert.assertTrue(logs.get(2).contains("Ticket Id="));

    }
}
