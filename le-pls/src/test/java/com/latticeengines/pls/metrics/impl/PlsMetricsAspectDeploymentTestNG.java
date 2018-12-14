package com.latticeengines.pls.metrics.impl;

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

import javax.inject.Inject;

import org.apache.commons.codec.digest.DigestUtils;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.pls.controller.ModelSummaryResource;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class PlsMetricsAspectDeploymentTestNG extends PlsFunctionalTestNGBase {

    @Inject
    private ModelSummaryResource modelSummaryResource;

    @Inject
    private GlobalAuthenticationService globalAuthenticationService;

    private Logger origLog;
    private ModelSummaryProxy origProxy;

    @BeforeClass(groups = "functional")
    public void beforeClass() throws Exception {
        origLog = PlsMetricsAspectImpl.log;
        origProxy = modelSummaryResource.getModelSummaryProxy();

        super.setup();
    }

    @AfterClass(groups = "functional")
    public void afterClass() {
        PlsMetricsAspectImpl.log = origLog;
        modelSummaryResource.setModelSummaryProxy(origProxy);
    }

    @Test(groups = "functional")
    public void logMetrics() {
        Logger newLog = mock(Logger.class);
        PlsMetricsAspectImpl.log = newLog;

        String testUser = TestFrameworkUtils.SUPER_ADMIN_USERNAME;

        final List<String> logs = new ArrayList<>();
        doAnswer((Answer<Object>) invocation -> {
            Object[] params = invocation.getArguments();
            logs.add((String) params[0]);
            return logs;
        }).when(newLog).debug(any());

        setupSecurityContext(mainTestTenant, testUser);
        modelSummaryResource.setModelSummaryProxy(mock(ModelSummaryProxy.class));

        modelSummaryResource.delete("1");
        verify(newLog, times(1)).debug(anyString());
        Assert.assertTrue(logs.get(0).contains("Metrics for API=ModelSummaryResource.delete(..) ElapsedTime="));
        Assert.assertTrue(logs.get(0).contains("Track Id="));
        Assert.assertTrue(logs.get(0).contains("User=" + testUser));

        modelSummaryResource.getModelSummaries(null);
        verify(newLog, times(2)).debug(anyString());

        verify(newLog, times(2)).debug(anyString());

        String passwd = DigestUtils.sha256Hex(TestFrameworkUtils.GENERAL_PASSWORD);
        Ticket ticket = globalAuthenticationService.authenticateUser(testUser, passwd);
        assertNotNull(ticket);
        assertTrue(ticket.getTenants().size() >= 2);
        boolean result = globalAuthenticationService.discard(ticket);
        assertTrue(result);
    }
}
