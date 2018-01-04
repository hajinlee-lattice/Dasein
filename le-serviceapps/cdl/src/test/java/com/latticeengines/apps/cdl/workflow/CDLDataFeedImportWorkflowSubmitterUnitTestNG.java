package com.latticeengines.apps.cdl.workflow;

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

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.service.TenantService;

public class CDLDataFeedImportWorkflowSubmitterUnitTestNG {
    private static final Logger log = LoggerFactory.getLogger(CDLDataFeedImportWorkflowSubmitterUnitTestNG.class);

    @Mock
    private InternalResourceRestApiProxy internalResourceProxy;

    @Mock
    private TenantService tenantService;

    @InjectMocks
    private CDLDataFeedImportWorkflowSubmitter cdlDataFeedImportWorkflowSubmitter;

    private String tenantId = "tenant";

    private Tenant tenant;

    private CustomerSpace cs;

    private DataFeedTask dataFeedTask;

    private CSVImportFileInfo csvImportFileInfo;

    private String INITIATOR = "test@lattice-engines.com";

    private String UNIQUE_ID = "dataFeed_ID";

    @BeforeClass(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockInternalProxy();
        mockActionInfo();
    }

    private void mockActionInfo() {
        tenant = new Tenant();
        tenant.setId(tenantId);
        cs = CustomerSpace.parse(tenantId);
        dataFeedTask = new DataFeedTask();
        dataFeedTask.setUniqueId(UNIQUE_ID);
        csvImportFileInfo = new CSVImportFileInfo();
        csvImportFileInfo.setFileUploadInitiator(INITIATOR);
        when(tenantService.findByTenantId(anyString())).thenReturn(tenant);
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
        cdlDataFeedImportWorkflowSubmitter.setInternalResourceRestApiProxy(internalResourceProxy);
    }

    @Test(groups = "unit")
    public void testRegisterAction() {
        Action action = cdlDataFeedImportWorkflowSubmitter.registerAction(cs, dataFeedTask, csvImportFileInfo);
        Assert.assertNotNull(action);
        Assert.assertEquals(action.getType(), ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        Assert.assertEquals(action.getActionInitiator(), INITIATOR);
        Assert.assertEquals(action.getTenant().getId(), tenantId);
        Assert.assertNull(action.getTrackingId());
        Assert.assertNotNull(action.getUpdated());
        Assert.assertNotNull(action.getCreated());
        log.info(String.format("Action is %s", action));
    }
}
