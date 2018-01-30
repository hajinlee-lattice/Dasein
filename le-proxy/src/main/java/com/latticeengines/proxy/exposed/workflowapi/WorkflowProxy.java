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
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component
public class WorkflowProxy extends MicroserviceRestApiProxy {

    private static Logger log = LoggerFactory.getLogger(WorkflowProxy.class);
    private static final String CUSTOMER_SPACE_ERROR = "No customer space provided.";

    public WorkflowProxy() {
        super("workflowapi/workflows");
    }

    public AppSubmission submitWorkflowExecution(WorkflowConfiguration workflowConfig, String customerSpace) {
        checkCustomerSpace(customerSpace);
        String baseUrl = "/jobs";
        String url = parseOptionalParameter(baseUrl, "customerSpace", customerSpace);
        url = constructUrl(url);
        log.info(String.format("Sending POST requst to %s, workflowConfig=%s, customerSpace=%s",
                url, JsonUtils.serialize(workflowConfig), customerSpace));
        return post("submitWorkflowExecution", url, workflowConfig, AppSubmission.class);
    }

    public AppSubmission submitWorkflowExecution(WorkflowConfiguration workflowConfig, String ... params) {
        String baseUrl = "/jobs";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        url = constructUrl(url);
        log.info(String.format("Sending POST request to %s, workflowConfig=%s, params=%s",
                url, JsonUtils.serialize(workflowConfig), JsonUtils.serialize(params)));
        return post("submitWorkflowExecution", url, workflowConfig, AppSubmission.class);
    }

    public String submitAWSWorkflowExecution(WorkflowConfiguration workflowConfig, String customerSpace) {
        checkCustomerSpace(customerSpace);
        String baseUrl = "/awsJobs";
        String url = parseOptionalParameter(baseUrl, "customerSpace", customerSpace);
        url = constructUrl(url);
        log.info(String.format("Sending POST request to %s, workflowConfig=%s, customerSpace=%s",
                url, JsonUtils.serialize(workflowConfig), customerSpace));
        return post("submitAWSWorkflowExecution", url, workflowConfig, String.class);
    }

    public String submitAWSWorkflowExecution(WorkflowConfiguration workflowConfig, String ... params) {
        String baseUrl = "/awsJobs";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        url = constructUrl(url);
        log.info(String.format("Sending POST request to %s, workflowConfig=%s, params=%s",
                url, JsonUtils.serialize(workflowConfig), JsonUtils.serialize(params)));
        return post("submitAWSWorkflowExecution", url, workflowConfig, String.class);
    }

    public AppSubmission restartWorkflowExecution(String workflowId, String customerSpace) {
        checkCustomerSpace(customerSpace);
        String baseUrl = "/job/{workflowId}/restart";
        String url = parseOptionalParameter(baseUrl, "customerSpace", customerSpace);
        url = constructUrl(url, workflowId);
        log.info(String.format("Sending POST request to %s, workflowId=%s, customerSpace=%s",
                url, workflowId, customerSpace));
        return post("restartWorkflowExecution", url, null, AppSubmission.class);
    }

    public AppSubmission restartWorkflowExecution(String workflowId, String ... params) {
        String baseUrl = "/job/{workflowId}/restart";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        url = constructUrl(url, workflowId);
        log.info(String.format("Sending POST request to %s, workflowId=%s, params=%s",
                url, workflowId, JsonUtils.serialize(params)));
        return post("restartWorkflowExecution", url, null, AppSubmission.class);
    }

    public void stopWorkflowExecution(String workflowId, String customerSpace) {
        checkCustomerSpace(customerSpace);
        String baseUrl = "/job/{workflowId}/stop";
        String url = parseOptionalParameter(baseUrl, "customerSpace", customerSpace);
        url = constructUrl(url, workflowId);
        log.info(String.format("Sending POST request to %s, workflowId=%s, customerSpace=%s",
                url, workflowId, customerSpace));
        post("stopWorkflowExecution", url, null, Void.class);
    }

    public void stopWorkflowExecution(String workflowId, String ... params) {
        String baseUrl = "/job/{workflowId}/stop";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        url = constructUrl(url, workflowId);
        log.info(String.format("Sending POST request to %s, workflowId=%s, params=%s",
                url, workflowId, JsonUtils.serialize(params)));
        post("stopWorkflowExecution", url, null, Void.class);
    }

    public WorkflowExecutionId getWorkflowId(String applicationId, String customerSpace) {
        checkCustomerSpace(customerSpace);
        String baseUrl = "/yarnapps/id/{applicationId}";
        String url = parseOptionalParameter(baseUrl, "customerSpace", customerSpace);
        url = constructUrl(url, applicationId);
        log.info(String.format("Sending GET reqeust to %s, applicationId=%s customerSpace=%s",
                url, applicationId, customerSpace));
        return get("getWorkflowId", url, WorkflowExecutionId.class);
    }

    public WorkflowExecutionId getWorkflowId(String applicationId, String ... params) {
        String baseUrl = "/yarnapps/id/{applicationId}";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        url = constructUrl(url, applicationId);
        log.info(String.format("Sending GET request to %s, applicationId=%s, params=%s",
                url, applicationId, JsonUtils.serialize(params)));
        return get("getWorkflowId", url, WorkflowExecutionId.class);
    }

    public WorkflowStatus getWorkflowStatus(String workflowId, String customerSpace) {
        checkCustomerSpace(customerSpace);
        String baseUrl = "/status/{workflowId}";
        String url = parseOptionalParameter(baseUrl, "customerSpace", customerSpace);
        url = constructUrl(url, workflowId);
        log.info(String.format("Sending GET request to %s, workflowId=%s, customerSpace=%s",
                url, workflowId, customerSpace));
        return get("getWorkflowStatus", url, WorkflowStatus.class);
    }

    public WorkflowStatus getWorkflowStatus(String workflowId, String ... params) {
        String baseUrl = "/status/{workflowId}";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        url = constructUrl(url, workflowId);
        log.info(String.format("Sending GET request to %s, workflowId=%s, params=%s",
                url, workflowId, JsonUtils.serialize(params)));
        return get("getWorkflowStatus", url, WorkflowStatus.class);
    }

    public Job getWorkflowJobFromApplicationId(String applicationId, String customerSpace) {
        checkCustomerSpace(customerSpace);
        String baseUrl = "/yarnapps/job/{applicationId}";
        String url = parseOptionalParameter(baseUrl, "customerSpace", customerSpace);
        url = constructUrl(url, applicationId);
        log.info(String.format("Sending GET request to %s, applicationId=%s, customerSpace=%s",
                url, applicationId, customerSpace));
        return get("getJobFromApplicationId", url, Job.class);
    }

    public Job getWorkflowJobFromApplicationId(String applicationId, String... params) {
        String baseUrl = "/yarnapps/job/{applicationId}";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        url = constructUrl(url, applicationId);
        log.info(String.format("Sending GET request to %s, applicationId=%s, params=%s",
                url, applicationId, JsonUtils.serialize(params)));
        return get("getJobFromApplicationId", url, Job.class);
    }

    public Job getWorkflowExecution(String workflowId, String customerSpace) {
        checkCustomerSpace(customerSpace);
        String baseUrl = "/job/{workflowId}";
        String url = parseOptionalParameter(baseUrl, "customerSpace", customerSpace);
        url = constructUrl(url, workflowId);
        log.info(String.format("Sending GET request to %s, workflowId=%s, customerSpace=%s",
                url, workflowId, customerSpace));
        return get("getJobFromWorkflowId", url, Job.class);
    }

    public Job getWorkflowExecution(String workflowId, String ... params) {
        String baseUrl = "/job/{workflowId}";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        url = constructUrl(url, workflowId);
        log.info(String.format("Sending GET request to %s, workflowId=%s, params=%s",
                url, workflowId, JsonUtils.serialize(params)));
        return get("getJobFromWorkflowId", url, Job.class);
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
        log.info(String.format("Sending GET request to %s, jobIds=%s, customerSpace=%s",
                url, JsonUtils.serialize(jobIds), customerSpace));
        return JsonUtils.convertList(get("getJobs", url, List.class), Job.class);
    }

    public List<Job> getWorkflowExecutionsByJobIds(List<String> jobIds, String ... params) {
        if (CollectionUtils.isEmpty(jobIds)) {
            return Collections.emptyList();
        }

        String baseUrl = "/jobs";
        String url = parseOptionalParameter(baseUrl, "customerSpace", params);
        url += url.contains("?customerSpace=") ? "&" : "?";
        url += buildQueryString("jobId", jobIds);
        url = constructUrl(url);
        log.info(String.format("Sending GET request to %s, jobIds=%s, params=%s",
                url, JsonUtils.serialize(jobIds), JsonUtils.serialize(params)));
        return JsonUtils.convertList(get("getJobs", url, List.class), Job.class);
    }

    public List<Job> getWorkflowExecutionsForTenant(Tenant tenant, String ... params) {
        String customerSpace = shortenCustomerSpace(CustomerSpace.parse(tenant.getId()).toString());
        checkCustomerSpace(customerSpace);
        String baseUrl = "/jobs";
        StringBuilder urlBuilder = new StringBuilder(baseUrl);
        urlBuilder.append("?customerSpace=").append(customerSpace);
        if (params != null && params.length > 0) {
            urlBuilder.append("&type=").append(params[0]);
        }
        String url = constructUrl(urlBuilder.toString());
        log.info(String.format("Sending GET request to %s, tenant=%s, params=%s",
                url, JsonUtils.serialize(tenant), JsonUtils.serialize(params)));
        return JsonUtils.convertList(get("getWorkflowExecutionsForTenant", url, List.class), Job.class);
    }

    public List<Job> getJobs(List<String> jobIds, List<String> types, Boolean includeDetails, String customerSpace) {
        checkCustomerSpace(customerSpace);
        String baseUrl = "/jobs";
        String url = generateGetWorkflowUrls(baseUrl, customerSpace, jobIds, types, includeDetails, false);
        log.info(String.format("Sending GET request to %s, jobIds=%s, types=%s, includeDetails=%s, customerSpace=%s",
                url, JsonUtils.serialize(jobIds), JsonUtils.serialize(types), JsonUtils.serialize(includeDetails),
                customerSpace));
        return JsonUtils.convertList(get("getJobs", url, List.class), Job.class);
    }

    public List<Job> getJobs(List<String> jobIds, List<String> types, Boolean includeDetails, String ... params) {
        String baseUrl = "/jobs";
        if (params != null && params.length > 0) {
            String customerSpace = params[0];
            String url = generateGetWorkflowUrls(baseUrl, customerSpace, jobIds, types, includeDetails, false);
            log.info(String.format("Sending GET request to %s, jobIds=%s, types=%s, includeDetails=%s, params=%s",
                    url, JsonUtils.serialize(jobIds), JsonUtils.serialize(types), JsonUtils.serialize(includeDetails),
                    JsonUtils.serialize(params)));
            return JsonUtils.convertList(get("getJobs", url, List.class), Job.class);
        } else {
            String url = generateGetWorkflowUrls(baseUrl,null, jobIds, types, includeDetails, false);
            log.info(String.format("Sending GET request to %s, jobIds=%s, types=%s, includeDetails=%s, params=%s",
                    url, JsonUtils.serialize(jobIds), JsonUtils.serialize(types), JsonUtils.serialize(includeDetails),
                    JsonUtils.serialize(params)));
            return JsonUtils.convertList(get("getJobs", url, List.class), Job.class);
        }
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
        log.info(String.format("Sending PUT request to %s, jobIds=%s, parentJobId=%s, customerSpace=%s",
                url, JsonUtils.serialize(jobIds), parentJobId, customerSpace));
        put("updateParentJobId", url, null);
    }

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
            log.info(String.format("Sending PUT request to %s, jobIds=%s, parentJobId=%s, params=%s",
                    url, JsonUtils.serialize(jobIds), parentJobId, JsonUtils.serialize(params)));
            put("updateParentJobId", url, null);
        } else {
            String url = generateUpdateParentJobIdUrl(baseUrl, "", jobIds, parentJobId);
            log.info(String.format("Sending PUT request to %s, jobIds=%s, parentJobId=%s, params=%s",
                    url, JsonUtils.serialize(jobIds), parentJobId, JsonUtils.serialize(params)));
            put("updateParentJobId", url, null);
        }
    }

    private void checkCustomerSpace(String customerSpace) {
        if (StringUtils.isBlank(customerSpace)) {
            throw new RuntimeException(CUSTOMER_SPACE_ERROR);
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
