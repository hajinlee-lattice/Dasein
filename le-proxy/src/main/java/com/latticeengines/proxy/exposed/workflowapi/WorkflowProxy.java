package com.latticeengines.proxy.exposed.workflowapi;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.JobRequest;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflowapi.WorkflowLogLinks;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component
public class WorkflowProxy extends MicroserviceRestApiProxy {

    private static final String CUSTOMER_SPACE_ERROR = "No customer space provided.";

    public WorkflowProxy() {
        super("workflowapi/workflows");
    }

    public WorkflowProxy(String hostport) {
        super(hostport, "workflowapi/workflows");
    }

    public AppSubmission submitWorkflow(WorkflowConfiguration workflowConfig, String customerSpace, Long workflowPid) {
        checkCustomerSpace(customerSpace);
        String baseUrl = "/jobs/submitwithpid";
        String url = parseOptionalParameter(baseUrl, "customerSpace", customerSpace);
        url += ("&workflowPid=" + workflowPid);
        url = constructUrl(url);
        return post("submitWorkflow", url, workflowConfig, AppSubmission.class);
    }

    public AppSubmission submitWorkflow(WorkflowConfiguration workflowConfig, Long workflowPid, String... params) {
        String baseUrl = "/jobs/submitwithpid";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        if (url.contains("customerSpace=")) {
            url += ("&workflowPid=" + workflowPid);
        } else {
            url += ("?workflowPid=" + workflowPid);
        }
        url = constructUrl(url);
        return post("submitWorkflow", url, workflowConfig, AppSubmission.class);
    }

    public AppSubmission submitWorkflowExecution(WorkflowConfiguration workflowConfig, String customerSpace) {
        checkCustomerSpace(customerSpace);
        String baseUrl = "/jobs/submit";
        String url = parseOptionalParameter(baseUrl, "customerSpace", customerSpace);
        url = constructUrl(url);
        return post("submitWorkflowExecution", url, workflowConfig, AppSubmission.class);
    }

    public AppSubmission submitWorkflowExecution(WorkflowConfiguration workflowConfig, String... params) {
        String baseUrl = "/jobs/submit";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        url = constructUrl(url);
        return post("submitWorkflowExecution", url, workflowConfig, AppSubmission.class);
    }

    public String submitAWSWorkflowExecution(WorkflowConfiguration workflowConfig, String customerSpace) {
        checkCustomerSpace(customerSpace);
        String baseUrl = "/awsJobs/submit";
        String url = parseOptionalParameter(baseUrl, "customerSpace", customerSpace);
        url = constructUrl(url);
        return post("submitAWSWorkflowExecution", url, workflowConfig, String.class);
    }

    public String submitAWSWorkflowExecution(WorkflowConfiguration workflowConfig, String... params) {
        String baseUrl = "/awsJobs/submit";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        url = constructUrl(url);
        return post("submitAWSWorkflowExecution", url, workflowConfig, String.class);
    }

    public Long createWorkflowJob(String customerSpace) {
        checkCustomerSpace(customerSpace);
        String baseUrl = "/jobs/create";
        String url = parseOptionalParameter(baseUrl, "customerSpace", customerSpace);
        url = constructUrl(url);
        return post("createWorkflowJob", url, null, Long.class);
    }

    public Long createFailedWorkflowJob(String customerSpace, Job failedJob) {
        checkCustomerSpace(customerSpace);
        String baseUrl = "/jobs/createfail";
        String url = parseOptionalParameter(baseUrl, "customerSpace", customerSpace);
        url = constructUrl(url);
        return post("createWorkflowJob", url, failedJob, Long.class);
    }

    public AppSubmission restartWorkflowExecution(String workflowId, String customerSpace) {
        checkCustomerSpace(customerSpace);
        return restartWorkflowExecution(workflowId, customerSpace, null);
    }

    public AppSubmission restartWorkflowExecution(String workflowId, String customerSpace, Integer memory) {
        checkCustomerSpace(customerSpace);
        return restartWorkflowExecution(workflowId, customerSpace, memory, null);
    }

    public AppSubmission restartWorkflowExecution(String workflowId, String customerSpace, Integer memory,
                                                  Boolean autoRetry) {
        checkCustomerSpace(customerSpace);

        String baseUrl = "/job/{workflowId}/restart";
        String url = parseOptionalParameter(baseUrl, "customerSpace", customerSpace);
        if (memory != null) {
            url += "&memory=" + memory;
        }
        if (autoRetry != null) {
            url += "&autoRetry=" + autoRetry;
        }
        url = constructUrl(url, workflowId);
        return post("restartWorkflowExecution", url, null, AppSubmission.class);
    }

    public void stopWorkflowExecution(String workflowId, String customerSpace) {
        checkCustomerSpace(customerSpace);
        String baseUrl = "/job/{workflowId}/stop";
        String url = parseOptionalParameter(baseUrl, "customerSpace", customerSpace);
        url = constructUrl(url, workflowId);
        post("stopWorkflowExecution", url, null, Void.class);
    }

    public void stopWorkflowExecution(String workflowId, String... params) {
        String baseUrl = "/job/{workflowId}/stop";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        url = constructUrl(url, workflowId);
        post("stopWorkflowExecution", url, null, Void.class);
    }

    public WorkflowExecutionId getWorkflowId(String applicationId, String customerSpace) {
        checkCustomerSpace(customerSpace);
        String baseUrl = "/yarnapps/id/{applicationId}";
        String url = parseOptionalParameter(baseUrl, "customerSpace", customerSpace);
        url = constructUrl(url, applicationId);
        return get("getWorkflowId", url, WorkflowExecutionId.class);
    }

    public WorkflowExecutionId getWorkflowId(String applicationId, String... params) {
        String baseUrl = "/yarnapps/id/{applicationId}";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        url = constructUrl(url, applicationId);
        return get("getWorkflowId", url, WorkflowExecutionId.class);
    }

    public Job getWorkflowJobFromApplicationId(String applicationId, String customerSpace) {
        checkCustomerSpace(customerSpace);
        String baseUrl = "/yarnapps/job/{applicationId}";
        String url = parseOptionalParameter(baseUrl, "customerSpace", customerSpace);
        url = constructUrl(url, applicationId);
        return get("getJobFromApplicationId", url, Job.class);
    }

    public Job getWorkflowJobFromApplicationId(String applicationId, String... params) {
        String baseUrl = "/yarnapps/job/{applicationId}";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        url = constructUrl(url, applicationId);
        return get("getJobFromApplicationId", url, Job.class);
    }

    public Job getWorkflowExecution(String workflowId, String customerSpace) {
        checkCustomerSpace(customerSpace);
        String baseUrl = "/job/{workflowId}";
        String url = parseOptionalParameter(baseUrl, "customerSpace", customerSpace);
        url = constructUrl(url, workflowId);
        return get("getJobFromWorkflowId", url, Job.class);
    }

    public Job getWorkflowExecution(String workflowId, String... params) {
        return getWorkflowExecution(workflowId, false, params);
    }

    public Job getWorkflowExecution(String workflowId, boolean bypassCache, String... params) {
        String baseUrl = "/job/{workflowId}";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        if (bypassCache) {
            url += url.contains("?customerSpace=") ? "&" : "?";
            url += buildQueryString("bypassCache", Collections.singletonList("true"));
        }
        url = constructUrl(url, workflowId);
        return get("getJobFromWorkflowId", url, Job.class);
    }

    public void setErrorCategoryByJobPid(String workflowPid, String errorCategory, String customerSpace) {
        String baseUrl = "/job/{workflowPid}/setErrorCategory?customerSpace={customerSpace}&errorCategory={errorCategory}";
        String url = constructUrl(baseUrl, workflowPid, customerSpace, errorCategory);
        get("setErrorCategoryByJobPid", url, Void.class);
    }

    public List<Job> getWorkflowExecutionsByJobIds(List<String> jobIds, String customerSpace) {
        checkCustomerSpace(customerSpace);
        if (CollectionUtils.isEmpty(jobIds)) {
            return Collections.emptyList();
        }

        String baseUrl = "/jobs";
        String url = parseOptionalParameter(baseUrl, "customerSpace", customerSpace);
        url += url.contains("?customerSpace=") ? "&" : "?";
        url += buildQueryString("jobId", jobIds);
        url = constructUrl(url);
        return JsonUtils.convertList(get("getJobs", url, List.class), Job.class);
    }

    public List<Job> getWorkflowExecutionsByJobIds(List<String> jobIds, String... params) {
        if (CollectionUtils.isEmpty(jobIds)) {
            return Collections.emptyList();
        }

        String baseUrl = "/jobs";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        url += url.contains("?customerSpace=") ? "&" : "?";
        url += buildQueryString("jobId", jobIds);
        url = constructUrl(url);
        return JsonUtils.convertList(get("getJobs", url, List.class), Job.class);
    }

    public Integer getNonTerminalJobCount(@NotNull String customerSpace, List<String> types) {
        Preconditions.checkNotNull(customerSpace);
        String url = "/clusters/current/jobs/count?customerSpace=" + customerSpace;
        if (CollectionUtils.isNotEmpty(types)) {
            url += "&type=" + String.join(",", types);
        }
        url = constructUrl(url);
        return get("getNonTerminalJobCount", url, Integer.class);
    }

    public List<Job> getWorkflowExecutionsByJobPids(List<String> jobIds, String customerSpace) {
        checkCustomerSpace(customerSpace);
        if (CollectionUtils.isEmpty(jobIds)) {
            return Collections.emptyList();
        }

        String baseUrl = "/jobsByPid";
        JobRequest request = new JobRequest();
        request.setCustomerSpace(customerSpace);
        request.setJobIds(jobIds);
        String url = constructUrl(baseUrl);
        return JsonUtils.convertList(post("getJobsByPid", url, request, List.class), Job.class);
    }

    public List<Job> getWorkflowExecutionsByJobPids(List<String> jobIds, String... params) {
        if (CollectionUtils.isEmpty(jobIds)) {
            return Collections.emptyList();
        }

        String baseUrl = "/jobsByPid";
        JobRequest request = new JobRequest();
        request.setJobIds(jobIds);
        if (params != null && params.length > 0) {
            request.setCustomerSpace(shortenCustomerSpace(params[0]));
        }

        String url = constructUrl(baseUrl);
        return JsonUtils.convertList(post("getJobsByPid", url, request, List.class), Job.class);
    }

    public List<Job> getWorkflowExecutionsForTenant(Tenant tenant, String... params) {
        String customerSpace = shortenCustomerSpace(CustomerSpace.parse(tenant.getId()).toString());
        checkCustomerSpace(customerSpace);
        String baseUrl = "/jobs";
        StringBuilder urlBuilder = new StringBuilder(baseUrl);
        urlBuilder.append("?customerSpace=").append(customerSpace);
        if (params != null && params.length > 0) {
            urlBuilder.append("&type=").append(params[0]);
        }
        String url = constructUrl(urlBuilder.toString());
        return JsonUtils.convertList(get("getWorkflowExecutionsForTenant", url, List.class), Job.class);
    }

    public List<Job> getJobs(List<String> jobIds, List<String> types, Boolean includeDetails, String customerSpace) {
        checkCustomerSpace(customerSpace);
        return getJobs(jobIds, types, null, includeDetails, customerSpace);
    }

    public List<Job> getJobs(List<String> jobIds, List<String> types, Boolean includeDetails, String... params) {
        return getJobs(jobIds, types, null, includeDetails, params);
    }

    public List<Job> getJobs(List<String> jobIds, List<String> types, List<String> jobStatuses, Boolean includeDetails, String... params) {
        String baseUrl = "/jobs";
        String customerSpace = null;
        if (params != null && params.length > 0) {
            customerSpace = params[0];
        }
        String url = generateGetWorkflowUrls(baseUrl, customerSpace, jobIds, types, jobStatuses, includeDetails, false);
        return JsonUtils.convertList(get("getJobs", url, List.class), Job.class);
    }

    public void updateParentJobId(List<String> jobIds, String parentJobId, String customerSpace) {
        if (CollectionUtils.isEmpty(jobIds)) {
            throw new LedpException(LedpCode.LEDP_18165);
        }

        if (StringUtils.isBlank(parentJobId)) {
            throw new LedpException(LedpCode.LEDP_18166);
        }

        checkCustomerSpace(customerSpace);
        String baseUrl = "/jobs";
        String url = generateUpdateParentJobIdUrl(baseUrl, customerSpace, jobIds, parentJobId);
        put("updateParentJobId", url);
    }

    public void updateParentJobId(List<String> jobIds, String parentJobId, String... params) {
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
            put("updateParentJobId", url);
        } else {
            String url = generateUpdateParentJobIdUrl(baseUrl, "", jobIds, parentJobId);
            put("updateParentJobId", url);
        }
    }

    public WorkflowLogLinks getLogLinkByWorkflowPid(long workflowPid) {
        String url = constructUrl("/log-link/pid/{pid}", workflowPid);
        return get("get log links", url, WorkflowLogLinks.class);
    }

    public List<WorkflowJob> queryByClusterIDAndTypesAndStatuses( String clusterId, List<String> types, List<String> statuses) {
        String baseUrl = "/jobsbycluster";
        String url = generateGetWorkflowUrls(baseUrl, clusterId, types, statuses);
        return JsonUtils.convertList(get("jobsByCluster", url, List.class), WorkflowJob.class);
    }

    public void deleteByTenantPid(String customerSpace, Long tenantPid) {
        checkCustomerSpace(customerSpace);

        String baseUrl = "/yarnapps/job/deletebytenant/{tenantPid}";
        String url = parseOptionalParameter(baseUrl, "customerSpace", customerSpace);
        url = constructUrl(url, tenantPid);
        delete("deleteByTenantPid", url);
    }

    private void checkCustomerSpace(String customerSpace) {
        if (StringUtils.isBlank(customerSpace)) {
            throw new RuntimeException(CUSTOMER_SPACE_ERROR);
        }
    }

    @VisibleForTesting
    String parseOptionalParameter(String baseUrl, String parameterName, String... parameterValues) {
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
        return generateGetWorkflowUrls(baseUrl, customerSpace, jobIds, types, null, includeDetails, hasParentId);
    }

    @VisibleForTesting
    String generateGetWorkflowUrls(String baseUrl, String customerSpace, List<String> jobIds, List<String> types,
            List<String> statuses, Boolean includeDetails, Boolean hasParentId) {
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
        if (CollectionUtils.isNotEmpty(statuses)) {
            urlStr.append(buildQueryString("status", statuses)).append("&");
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
    String generateGetWorkflowUrls(String baseUrl, String clusterId, List<String> types, List<String> statuses) {
        StringBuilder urlStr = new StringBuilder();
        urlStr.append(baseUrl);
        urlStr.append("?");

        if (StringUtils.isNotEmpty(clusterId)) {
            urlStr.append("clusterId=").append(clusterId).append("&");
        }
        if (CollectionUtils.isNotEmpty(types)) {
            urlStr.append(buildQueryString("type", types)).append("&");
        }
        if (CollectionUtils.isNotEmpty(statuses)) {
            urlStr.append(buildQueryString("status", statuses)).append("&");
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
