package com.latticeengines.cdl.workflow.listeners;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.Date;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

public class DataFeedTaskImportListenerUnitTestNG {
    private static final Logger log = LoggerFactory.getLogger(DataFeedTaskImportListenerUnitTestNG.class);

    @Mock
    private InternalResourceRestApiProxy internalResourceProxy;

    @InjectMocks
    private DataFeedTaskImportListener dataFeedTaskImportListener;

    private String tenantId = "tenant";

    private Long tenantPid = 1000L;

    private Long trackingId = 27706L;

    private WorkflowJob job;

    private String INITIATOR = "test@lattice-engines.com";

    @BeforeClass(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockInternalProxy();
        mockWorkflowJob();
    }

    private void mockWorkflowJob() {
        job = new WorkflowJob();
        Tenant tenant = new Tenant();
        tenant.setId(tenantId);
        job.setTenantId(tenantPid);
        job.setTenant(tenant);
        job.setUserId(INITIATOR);
        job.setWorkflowId(trackingId);
    }

    private void mockInternalProxy() {
        when(internalResourceProxy.createAction(anyString(), any(Action.class))).thenAnswer(new Answer<Action>() {
            @Override
            public Action answer(InvocationOnMock invocation) throws Throwable {
                Object[] arguments = invocation.getArguments();
                if (arguments != null && arguments.length == 2) {
                    Action action = (Action) arguments[1];
                    action.setCreated(new Date());
                    action.setUpdated(new Date());
                    return action;
                }
                return null;
            }
        });
        dataFeedTaskImportListener.setInternalResourceRestApiProxy(internalResourceProxy);
    }

    @Test(groups = "unit")
    public void testRegisterImportAction() {
        Action action = dataFeedTaskImportListener.registerImportAction(job);
        Assert.assertNotNull(action);
        Assert.assertEquals(action.getType(), ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        Assert.assertEquals(action.getActionInitiator(), INITIATOR);
        Assert.assertEquals(action.getTenant().getId(), tenantId);
        Assert.assertEquals(action.getTrackingId(), trackingId);
        Assert.assertNotNull(action.getUpdated());
        Assert.assertNotNull(action.getCreated());
        log.info(String.format("Action is %s", action));
    }
}
