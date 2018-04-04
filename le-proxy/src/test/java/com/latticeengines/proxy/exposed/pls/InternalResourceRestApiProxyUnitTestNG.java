package com.latticeengines.proxy.exposed.pls;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ActionType;

public class InternalResourceRestApiProxyUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(InternalResourceRestApiProxyUnitTestNG.class);
    private String localhost = "http://localhost:8081";
    private String expectedPrefix = "http://localhost:8081/pls/internal/jobs/all/";
    private InternalResourceRestApiProxy internalProxy = new InternalResourceRestApiProxy(localhost);

    private String tenantId = "tenant";

    private List<Long> actionIds = Arrays.asList(1L, 2L, 3L);

    private ActionType actionType = ActionType.CDL_DATAFEED_IMPORT_WORKFLOW;

    private String ratingEngineId = "ratingEngineId";

    private String modelId = "modelId";

    @Test(groups = "unit")
    public void testGenerateFindJobsBasedOnActionIdsAndTypeUrl() {
        String customerSpace = CustomerSpace.parse(tenantId).toString();
        String url = internalProxy.generateFindJobsBasedOnActionIdsAndTypeUrl(tenantId, null, null);
        Assert.assertEquals(url, expectedPrefix + customerSpace);
        url = internalProxy.generateFindJobsBasedOnActionIdsAndTypeUrl(tenantId, null, actionType);
        Assert.assertEquals(url, expectedPrefix + customerSpace + "?type=" + actionType.name());
        url = internalProxy.generateFindJobsBasedOnActionIdsAndTypeUrl(tenantId, actionIds, null);
        Assert.assertEquals(url, expectedPrefix + customerSpace + "?pid=1&pid=2&pid=3");
        url = internalProxy.generateFindJobsBasedOnActionIdsAndTypeUrl(tenantId, actionIds, actionType);
        Assert.assertEquals(url, expectedPrefix + customerSpace + "?pid=1&pid=2&pid=3&type=" + actionType.name());
    }

    @Test(groups = "unit")
    public void testUrl() {
        String url = internalProxy.constructUrlForDefaultABCDBucketsForCDL(CustomerSpace.parse(tenantId).toString(),
                ratingEngineId, modelId);
        log.info("url is " + url);
        Assert.assertEquals(url,
                "http://localhost:8081/pls/internal/bucketmetadata/ratingengine/ratingEngineId/model/modelId/tenant.tenant.Production");
    }

}
