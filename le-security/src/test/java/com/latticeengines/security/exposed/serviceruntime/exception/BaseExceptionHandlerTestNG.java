package com.latticeengines.security.exposed.serviceruntime.exception;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.message.BasicNameValuePair;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.monitor.exposed.alerts.service.AlertService;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;

public class BaseExceptionHandlerTestNG extends SecurityFunctionalTestNGBase {
    @Autowired
    @InjectMocks
    private TestExceptionHandler testExceptionHandler;

    @Mock
    private AlertService alertService;

    private final AtomicInteger count = new AtomicInteger();

    @BeforeClass
    @SuppressWarnings("unchecked")
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
        when(
                alertService.triggerCriticalEvent(any(String.class), any(String.class), any(String.class),
                        any(Iterable.class))).thenAnswer(new Answer<String>() {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable {
                count.incrementAndGet();
                Iterable<? extends BasicNameValuePair> details = (Iterable<? extends BasicNameValuePair>) invocation
                        .getArguments()[3];
                for (BasicNameValuePair pair : details) {
                    if (pair.getName().equals("tenant")) {
                        assertEquals(pair.getValue(), CustomerSpace.parse("BaseExceptionHandlerTestNG").toString());
                    }
                }
                return "{}";
            }
        });
    }

    @Test(groups = "unit")
    public void testPagerDuty() {
        int startValue = count.intValue();
        Tenant tenant = new Tenant();
        tenant.setId(CustomerSpace.parse("BaseExceptionHandlerTestNG").toString());
        MultiTenantContext.setTenant(tenant);
        try {
            throw new RuntimeException("Simulated Failure!");
        } catch (Exception e) {
            testExceptionHandler.handleException(e);
        }
        assertEquals(count.intValue(), startValue + 1);
    }

}
