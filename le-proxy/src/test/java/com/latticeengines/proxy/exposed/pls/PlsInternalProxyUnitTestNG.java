package com.latticeengines.proxy.exposed.pls;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ActionType;

public class PlsInternalProxyUnitTestNG {

    private String localhost = "http://localhost:8081";
    private String expectedPrefix = "http://localhost:8081/pls/internal/jobs/all/";
    private PlsInternalProxyImpl plsInternalProxy = new PlsInternalProxyImpl(localhost);
    private InternalResourceRestApiProxy internalResourceRestApiProxy = new InternalResourceRestApiProxy(localhost);

    private String tenantId = "tenant";

    private List<Long> actionIds = Arrays.asList(1L, 2L, 3L);

    private ActionType actionType = ActionType.CDL_DATAFEED_IMPORT_WORKFLOW;

    @Test(groups = "unit")
    public void testGenerateFindJobsBasedOnActionIdsAndTypeUrl() {
        String customerSpace = CustomerSpace.parse(tenantId).toString();
        String url = plsInternalProxy.generateFindJobsBasedOnActionIdsAndTypeUrl(tenantId, null, null);
        Assert.assertEquals(url, expectedPrefix + customerSpace);
        url = plsInternalProxy.generateFindJobsBasedOnActionIdsAndTypeUrl(tenantId, null, actionType);
        Assert.assertEquals(url, expectedPrefix + customerSpace + "?type=" + actionType.name());
        url = plsInternalProxy.generateFindJobsBasedOnActionIdsAndTypeUrl(tenantId, actionIds, null);
        Assert.assertEquals(url, expectedPrefix + customerSpace + "?pid=1&pid=2&pid=3");
        url = plsInternalProxy.generateFindJobsBasedOnActionIdsAndTypeUrl(tenantId, actionIds, actionType);
        Assert.assertEquals(url, expectedPrefix + customerSpace + "?pid=1&pid=2&pid=3&type=" + actionType.name());
    }

}
