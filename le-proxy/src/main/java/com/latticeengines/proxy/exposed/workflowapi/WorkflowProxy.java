package com.latticeengines.proxy.exposed.workflowapi;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import com.google.common.annotations.VisibleForTesting;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.network.exposed.workflowapi.WorkflowInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component
public class WorkflowProxy extends MicroserviceRestApiProxy implements WorkflowInterface {

    private static Logger log = LoggerFactory.getLogger(WorkflowProxy.class);

    public WorkflowProxy() {
        super("workflowapi/workflows");
    }

    @Override
    public AppSubmission submitWorkflowExecution(WorkflowConfiguration workflowConfig, String ... params) {
        String baseUrl = "/jobs";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        return post("submitWorkflowExecution", constructUrl(url), workflowConfig, AppSubmission.class);
    }

    @Override
    public String submitAWSWorkflowExecution(WorkflowConfiguration workflowConfig, String ... params) {
        String baseUrl = "/awsJobs";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        return post("submitAWSWorkflowExecution", constructUrl(url), workflowConfig, String.class);
    }

    @Override
    public AppSubmission restartWorkflowExecution(String workflowId, String ... params) {
        String baseUrl = "/job/{workflowId}/restart";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        return post("restartWorkflowExecution", constructUrl(url, workflowId), null, AppSubmission.class);
    }

    @Override
    public void stopWorkflowExecution(String workflowId, String ... params) {
        String baseUrl = "/job/{workflowId}/stop";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        post("stopWorkflowExecution", constructUrl(url, workflowId), null, Void.class);
    }

    @Override
    public WorkflowExecutionId getWorkflowId(String applicationId, String ... params) {
        String baseUrl = "/yarnapps/id/{applicationId}";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        return get("getWorkflowId", constructUrl(url, applicationId), WorkflowExecutionId.class);
    }

    @Override
    public WorkflowStatus getWorkflowStatus(String workflowId, String ... params) {
        String baseUrl = "/status/{workflowId}";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        return get("getWorkflowStatus", constructUrl(url, workflowId), WorkflowStatus.class);
    }

    @Override
    public Job getWorkflowJobFromApplicationId(String applicationId, String... params) {
        String baseUrl = "/yarnapps/job/{applicationId}";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        return get("getJobFromApplicationId", constructUrl(url, applicationId), Job.class);
    }

    @Override
    public Job getWorkflowExecution(String workflowId, String ... params) {
        String baseUrl = "/job/{workflowId}";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        return get("getJobFromWorkflowId", constructUrl(url, workflowId), Job.class);
    }

    @Override
    public List<Job> getWorkflowExecutionsByJobIds(List<String> jobIds, String ... params) {
        if (CollectionUtils.isEmpty(jobIds)) {
            return Collections.emptyList();
        }

        String baseUrl = "/jobs";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        url += url.contains("?customerSpace=") ? "&" : "?";
        url += buildQueryString("jobId", jobIds);
        return JsonUtils.convertList(get("getJobs", constructUrl(url), List.class), Job.class);
    }

    @Deprecated
    @Override
    public List<Job> getWorkflowExecutionsForTenant(Long tenantPid, String ... params) {
        String baseUrl = "/tenant/{tenantPid}/jobs";
        String url = parseOptionalParameter(baseUrl, "type", params);
        return JsonUtils.convertList(
                get("getWorkflowExecutionsForTenant", constructUrl(url, tenantPid), List.class), Job.class);
    }

    @Override
    public List<Job> getJobs(List<String> jobIds, List<String> types, Boolean includeDetails, String ... params) {
        String baseUrl = "/jobs";
        if (params != null && params.length > 0) {
            String customerSpace = params[0];
            String url = generateGetWorkflowUrls(baseUrl, customerSpace, jobIds, types, includeDetails, false);
            return JsonUtils.convertList(get("getJobs", url, List.class), Job.class);
        } else {
            String url = generateGetWorkflowUrls(baseUrl,null, jobIds, types, includeDetails, false);
            return JsonUtils.convertList(get("getJobs", url, List.class), Job.class);
        }
    }

    @Override
    public void updateParentJobId(List<String> jobIds, String parentJobId, String ... params) {
        if (CollectionUtils.isEmpty(jobIds)) {
            throw new LedpException(LedpCode.LEDP_18165);
        }

        if (StringUtils.isBlank(parentJobId)) {
            throw new LedpException(LedpCode.LEDP_18166);
        }

        String baseUrl = "/jobs";
        if (params != null && params.length > 0) {
            String customerSpace = params[0];
            String url = generateUpdateParentJobIdUrl(baseUrl, customerSpace, jobIds, parentJobId);
            put("updateParentJobId", url, null);
        } else {
            String url = generateUpdateParentJobIdUrl(baseUrl, "", jobIds, parentJobId);
            put("updateParentJobId", url, null);
        }
    }

    @VisibleForTesting
    String parseOptionalParameter(String baseUrl, String parameterName, String ... parameterValues) {
        if (parameterValues != null && parameterValues.length > 0) {
            String var = parameterValues[0];
            if (parameterName.equals("customerSpace")) {
                return String.format(baseUrl + "?%s=%s", parameterName, shortenCustomerSpace(var));
            } else {
                return String.format(baseUrl + "?%s=%s", parameterName, var);
            }
        } else {
            return baseUrl;
        }
    }

    @VisibleForTesting
    String buildQueryString(String queryStringName, List<String> parameters) {
        if (CollectionUtils.isEmpty(parameters)) {
            return StringUtils.EMPTY;
        }

        StringBuilder builder = new StringBuilder();
        parameters.forEach(parameter -> builder.append(String.format(queryStringName + "=%s&", parameter)));
        builder.setLength(builder.length() - 1);
        return builder.toString();
    }

    @VisibleForTesting
    String generateGetWorkflowUrls(String baseUrl, String customerSpace, List<String> jobIds, List<String> types,
                                   Boolean includeDetails, Boolean hasParentId) {
        StringBuilder urlStr = new StringBuilder();
        urlStr.append(baseUrl);
        if (StringUtils.isNotEmpty(customerSpace)) {
            urlStr.append("?customerSpace=").append(shortenCustomerSpace(customerSpace)).append("&");
        } else {
            urlStr.append("?");
        }
        if (CollectionUtils.isNotEmpty(jobIds)) {
            urlStr.append(buildQueryString("jobId", jobIds)).append("&");
        }
        if (CollectionUtils.isNotEmpty(types)) {
            urlStr.append(buildQueryString("type", types)).append("&");
        }
        if (includeDetails != null) {
            urlStr.append("includeDetails=").append(String.valueOf(includeDetails)).append("&");
        }
        if (hasParentId != null) {
            urlStr.append("hasParentId=").append(String.valueOf(hasParentId));
        }
        if (urlStr.charAt(urlStr.length() - 1) == '&') {
            urlStr.setLength(urlStr.length() - 1);
        }
        return constructUrl(urlStr.toString());
    }

    @VisibleForTesting
    String generateUpdateParentJobIdUrl(String baseUrl, String customerSpace, List<String> jobIds, String parentJobId) {
        StringBuilder urlStr = new StringBuilder();
        urlStr.append(baseUrl);
        if (StringUtils.isNotEmpty(customerSpace)) {
            urlStr.append("?customerSpace=").append(shortenCustomerSpace(customerSpace)).append("&");
        } else {
            urlStr.append("?");
        }
        urlStr.append(buildQueryString("jobId", jobIds)).append("&");
        urlStr.append("parentJobId=").append(parentJobId);
        return constructUrl(urlStr.toString());
    }
}
