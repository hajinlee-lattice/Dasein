package com.latticeengines.proxy.exposed.workflowapi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.proxy.exposed.BaseRestApiProxyTestNG;

public class WorkflowProxyTestNG extends BaseRestApiProxyTestNG {

    @Autowired
    private WorkflowProxy workflowProxy;

    private static final List<String> jobIds = Arrays.asList("12345", "12346", "123456");

    private static final String parentId = "12347";

    private static final List<String> types = Arrays.asList(
            "consolidateAndPublishWorkflow", "profileAndPublishWorkflow");

    private static final CustomerSpace customerSpace = CustomerSpace.parse("tenant");

    private static final String baseUrl = "/jobs";

    @Test(groups = "unit")
    public void testBuildQueryString() {
        Assert.assertEquals(workflowProxy.buildQueryString("anyName", null),
                StringUtils.EMPTY);

        Assert.assertEquals(workflowProxy.buildQueryString("anyName", new ArrayList<>()),
                StringUtils.EMPTY);

        List<String> parentJobId = Collections.singletonList(parentId);
        String parentJobIdString = workflowProxy.buildQueryString("parentJobId", parentJobId);
        Assert.assertEquals(parentJobIdString, "parentJobId=12347");

        String jobIdString = workflowProxy.buildQueryString("jobId", jobIds);
        Assert.assertEquals(jobIdString, "jobId=12345&jobId=12346&jobId=123456");

        String typeString = workflowProxy.buildQueryString("type", types);
        Assert.assertEquals(typeString, "type=consolidateAndPublishWorkflow&type=profileAndPublishWorkflow");
    }

    @Test(groups = "unit")
    public void testParseOptionalParameter() {
        String name = "customerSpace";
        Assert.assertEquals(workflowProxy.parseOptionalParameter(baseUrl, name, "t1"), "/jobs?customerSpace=t1");
        Assert.assertEquals(workflowProxy.parseOptionalParameter(baseUrl, name, "t1", "t2"), "/jobs?customerSpace=t1");
        Assert.assertEquals(workflowProxy.parseOptionalParameter(baseUrl, null), "/jobs");
        Assert.assertEquals(workflowProxy.parseOptionalParameter(baseUrl, name), "/jobs");
        name = "type";
        Assert.assertEquals(workflowProxy.parseOptionalParameter(baseUrl, name, "t1"), "/jobs?type=t1");
        Assert.assertEquals(workflowProxy.parseOptionalParameter(baseUrl, name, "t1", "t2"), "/jobs?type=t1");
    }

    @Test(groups = "functional")
    public void testGenerateGetWorkflowUrls() {
        String url = workflowProxy.generateGetWorkflowUrls(baseUrl, null, null, null, null, null);
        Assert.assertEquals(url, PropertyUtils.getProperty("common.microservice.url")
                + "/workflowapi/workflows/jobs");
        url = workflowProxy.generateGetWorkflowUrls(baseUrl, null, jobIds, null, true, null);
        Assert.assertEquals(url, PropertyUtils.getProperty("common.microservice.url")
                + "/workflowapi/workflows/jobs?jobId=12345&jobId=12346&jobId=123456&includeDetails=true");
        url = workflowProxy.generateGetWorkflowUrls(baseUrl, null, null, types, null, null);
        Assert.assertEquals(url, PropertyUtils.getProperty("common.microservice.url")
                + "/workflowapi/workflows/jobs?type=consolidateAndPublishWorkflow&type=profileAndPublishWorkflow");
        url = workflowProxy.generateGetWorkflowUrls(baseUrl, null, jobIds, types, true, null);
        Assert.assertEquals(url, PropertyUtils.getProperty("common.microservice.url")
                + "/workflowapi/workflows/jobs?jobId=12345&jobId=12346&jobId=123456&type=consolidateAndPublishWorkflow&type=profileAndPublishWorkflow&includeDetails=true");
        url = workflowProxy.generateGetWorkflowUrls(baseUrl, null, null, types, null, false);
        Assert.assertEquals(url, PropertyUtils.getProperty("common.microservice.url")
                + "/workflowapi/workflows/jobs?type=consolidateAndPublishWorkflow&type=profileAndPublishWorkflow&hasParentId=false");

        url = workflowProxy.generateGetWorkflowUrls(baseUrl, customerSpace.toString(), null, null, null, null);
        Assert.assertEquals(url, PropertyUtils.getProperty("common.microservice.url")
                + "/workflowapi/workflows/jobs?customerSpace=tenant");
        url = workflowProxy.generateGetWorkflowUrls(baseUrl, customerSpace.toString(), jobIds, null, null, null);
        Assert.assertEquals(url, PropertyUtils.getProperty("common.microservice.url")
                + "/workflowapi/workflows/jobs?customerSpace=tenant&jobId=12345&jobId=12346&jobId=123456");
        url = workflowProxy.generateGetWorkflowUrls(baseUrl, customerSpace.toString(), null, types, null, null);
        Assert.assertEquals(url, PropertyUtils.getProperty("common.microservice.url")
                + "/workflowapi/workflows/jobs?customerSpace=tenant&type=consolidateAndPublishWorkflow&type=profileAndPublishWorkflow");
        url = workflowProxy.generateGetWorkflowUrls(baseUrl, customerSpace.toString(), jobIds, types, null, null);
        Assert.assertEquals(url, PropertyUtils.getProperty("common.microservice.url")
                + "/workflowapi/workflows/jobs?customerSpace=tenant&jobId=12345&jobId=12346&jobId=123456&type=consolidateAndPublishWorkflow&type=profileAndPublishWorkflow");
        url = workflowProxy.generateGetWorkflowUrls(baseUrl, customerSpace.toString(), null, types, true, false);
        Assert.assertEquals(url, PropertyUtils.getProperty("common.microservice.url")
                + "/workflowapi/workflows/jobs?customerSpace=tenant&type=consolidateAndPublishWorkflow&type=profileAndPublishWorkflow&includeDetails=true&hasParentId=false");
    }

    @Test(groups = "functional")
    public void testGenerateUpdateParentJobIdUrl() {
        String url = workflowProxy.generateUpdateParentJobIdUrl(baseUrl, customerSpace.toString(), jobIds, parentId);
        Assert.assertEquals(url, PropertyUtils.getProperty("common.microservice.url") +
                "/workflowapi/workflows/jobs?customerSpace=tenant&jobId=12345&jobId=12346&jobId=123456&parentJobId=12347");
    }
}
