package com.latticeengines.pls.controller;

import java.util.Arrays;
import java.util.Collections;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.dataloader.LaunchJobsResult;
import com.latticeengines.domain.exposed.dataloader.QueryStatusResult;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.TenantDeployment;
import com.latticeengines.domain.exposed.pls.TenantDeploymentStatus;
import com.latticeengines.domain.exposed.pls.TenantDeploymentStep;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.TenantDeploymentManager;
import com.latticeengines.pls.service.TenantDeploymentService;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.util.SecurityUtils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "tenantdeployment", description = "REST resource for tenant deployment")
@RestController
@RequestMapping(value = "/tenantdeployments")
@PreAuthorize("hasRole('Edit_PLS_Configurations')")
public class TenantDeploymentResource {

    @Autowired
    private SessionService sessionService;

    @Autowired
    private TenantDeploymentService tenantDeploymentService;

    @Autowired
    private TenantDeploymentManager tenantDeploymentManager;

    @RequestMapping(value = "/deployment", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get tenant deployment")
    public ResponseDocument<TenantDeployment> getTenantDeployment(HttpServletRequest request) {
        ResponseDocument<TenantDeployment> response = new ResponseDocument<>();
        try
        {
            Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
            TenantDeployment deployment = tenantDeploymentService.getTenantDeployment(tenant.getId());

            response.setSuccess(true);
            response.setResult(deployment);
        } catch (Exception e) {
            response.setSuccess(false);
            response.setErrors(Arrays.asList(e.getMessage()));
        }

        return response;
    }

    @RequestMapping(value = "/deployment", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete tenant deployment")
    public SimpleBooleanResponse deleteDeployment(HttpServletRequest request) {
        try {
            Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
            tenantDeploymentService.deleteTenantDeployment(tenant.getId());

            return SimpleBooleanResponse.successResponse();
        } catch (LedpException e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(e.getMessage()));
        } catch (Exception e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(ExceptionUtils
                    .getFullStackTrace(e)));
        }
    }

    @RequestMapping(value = "/importsfdcdata", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Import Salesforce data")
    public SimpleBooleanResponse importSfdcData(HttpServletRequest request) {
        try {
            Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
            TenantDeployment deployment = tenantDeploymentService.getTenantDeployment(tenant.getId());
            if (deployment == null) {
                deployment = tenantDeploymentService.createTenantDeployment(tenant.getId(), request);
            }

            tenantDeploymentManager.importSfdcData(tenant.getId(), deployment);

            return SimpleBooleanResponse.successResponse();
        } catch (LedpException e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(e.getMessage()));
        } catch (Exception e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(ExceptionUtils
                    .getFullStackTrace(e)));
        }
    }

    @RequestMapping(value = "/enrichdata", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Enrich data")
    public SimpleBooleanResponse enrichData(HttpServletRequest request) {
        try {
            Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
            TenantDeployment deployment = tenantDeploymentService.getTenantDeployment(tenant.getId());
            tenantDeploymentManager.enrichData(tenant.getId(), deployment);

            return SimpleBooleanResponse.successResponse();
        } catch (LedpException e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(e.getMessage()));
        } catch (Exception e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(ExceptionUtils
                    .getFullStackTrace(e)));
        }
    }

    @RequestMapping(value = "/validatemetadata", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Validate metadata")
    public SimpleBooleanResponse validateMetadataData(HttpServletRequest request) {
        try {
            Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
            TenantDeployment deployment = tenantDeploymentService.getTenantDeployment(tenant.getId());
            tenantDeploymentManager.validateMetadata(tenant.getId(), deployment);

            return SimpleBooleanResponse.successResponse();
        } catch (LedpException e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(e.getMessage()));
        } catch (Exception e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(ExceptionUtils
                    .getFullStackTrace(e)));
        }
    }

    @RequestMapping(value = "/cancellaunch", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Cancel launch")
    public SimpleBooleanResponse cancelLaunch(HttpServletRequest request) {
        try {
            Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
            TenantDeployment deployment = tenantDeploymentService.getTenantDeployment(tenant.getId());
            tenantDeploymentManager.cancelLaunch(tenant.getId(), deployment.getCurrentLaunchId());

            return SimpleBooleanResponse.successResponse();
        } catch (LedpException e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(e.getMessage()));
        } catch (Exception e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(ExceptionUtils
                    .getFullStackTrace(e)));
        }
    }

    @RequestMapping(value = "/objects", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get extract objects")
    public ResponseDocument<LaunchJobsResult> getObjects(@RequestParam(value = "step") String step,
            @RequestParam(value = "status") String status, HttpServletRequest request) {
        ResponseDocument<LaunchJobsResult> response = new ResponseDocument<>();
        try
        {
            LaunchJobsResult jobs;
            Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
            if (TenantDeploymentStatus.valueOf(status) == TenantDeploymentStatus.IN_PROGRESS) {
                jobs = tenantDeploymentManager.getRunningJobs(tenant.getId(), TenantDeploymentStep.valueOf(step));
            } else {
                jobs = tenantDeploymentManager.getCompleteJobs(tenant.getId(), TenantDeploymentStep.valueOf(step));
            }

            response.setSuccess(true);
            response.setResult(jobs);
        } catch (Exception e) {
            response.setSuccess(false);
            response.setErrors(Arrays.asList(e.getMessage()));
        }

        return response;
    }

    @RequestMapping(value = "/successtime", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the last success time")
    public ResponseDocument<String> getSuccessTime(@RequestParam(value = "step") String step, HttpServletRequest request) {
        ResponseDocument<String> response = new ResponseDocument<>();
        try
        {
            Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
            String time = tenantDeploymentManager.getStepSuccessTime(tenant.getId(), TenantDeploymentStep.valueOf(step));
            response.setSuccess(true);
            response.setResult(time);
        } catch (Exception e) {
            response.setSuccess(false);
            response.setErrors(Arrays.asList(e.getMessage()));
        }

        return response;
    }

    @RequestMapping(value = "/runquery", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Run query")
    public ResponseDocument<String> runQuery(@RequestParam(value = "step") String step, HttpServletRequest request) {
        ResponseDocument<String> response = new ResponseDocument<>();
        try
        {
            Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
            String queryHandle = tenantDeploymentManager.runQuery(tenant.getId(), TenantDeploymentStep.valueOf(step));
            response.setSuccess(true);
            response.setResult(queryHandle);
        } catch (Exception e) {
            response.setSuccess(false);
            response.setErrors(Arrays.asList(e.getMessage()));
        }

        return response;
    }

    @RequestMapping(value = "/querystatus/{queryHandle}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get query status")
    public ResponseDocument<QueryStatusResult> getQueryStatus(@PathVariable String queryHandle, HttpServletRequest request) {
        ResponseDocument<QueryStatusResult> response = new ResponseDocument<>();
        try
        {
            Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
            QueryStatusResult result = tenantDeploymentManager.getQueryStatus(tenant.getId(), queryHandle);
            response.setSuccess(true);
            response.setResult(result);
        } catch (Exception e) {
            response.setSuccess(false);
            response.setErrors(Arrays.asList(e.getMessage()));
        }

        return response;
    }

    @RequestMapping(value = "/profilesummarycsv/{queryHandle}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get profile summary csv file")
    public void getProfileSummaryCsvFile(@PathVariable String queryHandle, HttpServletRequest request, HttpServletResponse response) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        tenantDeploymentManager.downloadQueryDataFile(request, response, "application/csv",
                tenant.getId(), queryHandle, "Data Profile Summary.csv");
    }

    @RequestMapping(value = "/enrichmentsummarycsv/{queryHandle}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get enrichment summary csv file")
    public void getEnrichmentSummaryCsvFile(@PathVariable String queryHandle, HttpServletRequest request, HttpServletResponse response) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        tenantDeploymentManager.downloadQueryDataFile(request, response, "application/csv",
                tenant.getId(), queryHandle, "Data Enrichment Summary.csv");
    }
}
