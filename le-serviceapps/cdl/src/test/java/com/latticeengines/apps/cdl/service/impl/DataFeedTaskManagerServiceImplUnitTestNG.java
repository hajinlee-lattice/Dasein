package com.latticeengines.apps.cdl.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.Date;

import org.apache.hadoop.yarn.util.ConverterUtils;
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

import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.service.TenantService;

public class DataFeedTaskManagerServiceImplUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(DataFeedTaskManagerServiceImplUnitTestNG.class);

    @Mock
    private TenantService tenantSevrice;

    @Mock
    private InternalResourceRestApiProxy internalResourceProxy;

    @InjectMocks
    private DataFeedTaskManagerServiceImpl dataFeedTaskManagerService;

    private String tenantId = "tenant";

    private String appId = "application_1511820474078_27706";

    private CSVImportFileInfo csvImportFileInfo;

    private String INITIATOR = "test@lattice-engines.com";

    @BeforeClass(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockTenantService();
        mockInternalProxy();
        mockCSVImportFileInfo();
    }

    private void mockCSVImportFileInfo() {
        csvImportFileInfo = new CSVImportFileInfo();
        csvImportFileInfo.setFileUploadInitiator(INITIATOR);
    }

    private void mockTenantService() {
        Tenant tenant = new Tenant();
        tenant.setId(tenantId);
        when(tenantSevrice.findByTenantId(anyString())).thenReturn(tenant);
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
        dataFeedTaskManagerService.setInternalResourceRestApiProxy(internalResourceProxy);
    }

    @Test(groups = "unit")
    public void testRegisterImportAction() {
        Action action = dataFeedTaskManagerService.registerImportAction(tenantId, ConverterUtils.toApplicationId(appId),
                csvImportFileInfo);
        Assert.assertNotNull(action);
        Assert.assertEquals(action.getType(), ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        Assert.assertEquals(action.getActionInitiator(), INITIATOR);
        Assert.assertEquals(action.getTenant().getId(), tenantId);
        Assert.assertEquals(action.getTrackingId(), appId);
        Assert.assertNotNull(action.getUpdated());
        Assert.assertNotNull(action.getCreated());
        log.info(String.format("Action is %s", action));
    }

}
