package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.workflow.exposed.WorkflowContextConstants;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "scores/jobs", description = "REST resource for retrieving job information for score operations")
@RestController
@RequestMapping("/scores/jobs")
public class ScoringJobResource {
    // TODO Rights

    private static final Log log = LogFactory.getLog(JobResource.class);

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @RequestMapping(value = "/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve all scoring jobs for the provided model id")
    public List<Job> findAll(@PathVariable String modelId) {
        Tenant tenantWithPid = getTenant();
        log.info("Finding jobs for " + tenantWithPid.toString() + " with pid " + tenantWithPid.getPid() + " and model "
                + modelId);
        List<Job> jobs = workflowProxy.getWorkflowExecutionsForTenant(tenantWithPid.getPid());
        List<Job> ret = new ArrayList<>();
        for (Job job : jobs) {
            if (job.getJobType().equals("scoreWorkflow")) {
                String jobModelId = job.getOutputs().get(WorkflowContextConstants.Outputs.MODEL_ID);
                if (jobModelId != null && jobModelId.equals(modelId)) {
                    ret.add(job);
                }
            }
        }
        return ret;
    }

    private Tenant getTenant() {
        Tenant tenant = MultiTenantContext.getTenant();
        return tenantEntityMgr.findByTenantId(tenant.getId());
    }
}
