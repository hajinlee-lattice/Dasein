package com.latticeengines.proxy.exposed.workflowapi;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.proxy.exposed.BaseRestApiProxyTestNG;

public class WorkflowProxyTestNG extends BaseRestApiProxyTestNG {

    @Autowired
    private WorkflowProxy workflowProxy;

    private static final List<String> jobIds = Arrays.asList("12345", "12346");

    private static final String parentId = "12347";

    private static final List<String> types = Arrays.asList("consolidateandpublishworkflow",
            "profileandpublishworkflow");

    private static final CustomerSpace customerSpace = CustomerSpace.parse("tenant");

    @Test(groups = "functional")
    public void testGenerateGetWorkflowUrls() {
        String url = workflowProxy.generateGetWorkflowUrls(customerSpace.toString(), null, null, null, null);
        Assert.assertEquals(url, PropertyUtils.getProperty("common.microservice.url")
                + "/workflowapi/workflows/customerspaces/tenant/jobs");
        url = workflowProxy.generateGetWorkflowUrls(customerSpace.toString(), jobIds, null, null, null);
        Assert.assertEquals(url, PropertyUtils.getProperty("common.microservice.url")
                + "/workflowapi/workflows/customerspaces/tenant/jobs?jobIds=12345&jobIds=12346");
        url = workflowProxy.generateGetWorkflowUrls(customerSpace.toString(), null, types, null, null);
        Assert.assertEquals(url, PropertyUtils.getProperty("common.microservice.url")
                + "/workflowapi/workflows/customerspaces/tenant/jobs?types=consolidateandpublishworkflow&types=profileandpublishworkflow");
        url = workflowProxy.generateGetWorkflowUrls(customerSpace.toString(), jobIds, types, null, null);
        Assert.assertEquals(url, PropertyUtils.getProperty("common.microservice.url")
                + "/workflowapi/workflows/customerspaces/tenant/jobs?jobIds=12345&jobIds=12346&types=consolidateandpublishworkflow&types=profileandpublishworkflow");
        url = workflowProxy.generateGetWorkflowUrls(customerSpace.toString(), null, types, null, false);
        Assert.assertEquals(url, PropertyUtils.getProperty("common.microservice.url")
                + "/workflowapi/workflows/customerspaces/tenant/jobs?types=consolidateandpublishworkflow&types=profileandpublishworkflow&hasParentId=false");
    }

    @Test(groups = "functional")
    public void testGenerateUpdateParentJobIdUrl() {
        String url = workflowProxy.generateUpdateParentJobIdUrl(customerSpace.toString(), jobIds, parentId);
        Assert.assertEquals(url, PropertyUtils.getProperty("common.microservice.url")
                + "/workflowapi/workflows/customerspaces/tenant/jobs?jobIds=12345&jobIds=12346&parentJobId=12347");
    }
}
