package com.latticeengines.pls.controller;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class RestApiMetricsAspectTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    ModelSummaryResource modelSummaryResource;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Test(groups = { "functional" })
    public void logMetrics() throws Exception {

        Log origLog = RestApiMetricsAspect.log;
        Log newLog = mock(Log.class);
        RestApiMetricsAspect.log = newLog;

        final List<String> logs = new ArrayList<String>();
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
        ModelSummaryEntityMgr origSummaryEntityMgr = modelSummaryResource.getModelSummaryEntityMgr();

        try {
            modelSummaryResource.delete("1");
            verify(newLog, times(1)).info(anyString());
            Assert.assertTrue(logs.get(0).contains("Metrics for API=delete ElapsedTime="));

            modelSummaryResource.getModelSummaries();
            verify(newLog, times(2)).info(anyString());

            modelSummaryResource.getModelSummaryEntityMgr();
            verify(newLog, times(2)).info(anyString());

        } finally {
            RestApiMetricsAspect.log = origLog;
            modelSummaryResource.setModelSummaryEntityMgr(origSummaryEntityMgr);
        }

    }
}
