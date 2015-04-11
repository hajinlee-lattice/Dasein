package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.impl.ModelSummaryParser;
import com.latticeengines.security.exposed.globalauth.GlobalSessionManagementService;
import com.latticeengines.security.exposed.util.SecurityUtils;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "modelsummary", description = "REST resource for model summaries")
@RestController
@RequestMapping("/modelsummaries")
@PreAuthorize("hasRole('View_PLS_Models')")
public class ModelSummaryResource {

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private GlobalSessionManagementService globalSessionManagementService;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private ModelSummaryParser modelSummaryParser;

    @RequestMapping(value = "/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get summary for specific model")
    public ModelSummary getModelSummary(@PathVariable String modelId) {
        ModelSummary summary = modelSummaryEntityMgr.findValidByModelId(modelId);
        if (summary != null) {
            summary.setPredictors(new ArrayList<Predictor>());
        }
        return summary;
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of model summary ids available to the user")
    public List<ModelSummary> getModelSummaries(@RequestParam(value="selection", required=false) String selection) {

        List<ModelSummary> summaries = null;
        if (selection != null && selection.equalsIgnoreCase("all")) {
            summaries = modelSummaryEntityMgr.findAll();
        } else {
            summaries = modelSummaryEntityMgr.findAllValid();
        }

        for (ModelSummary summary : summaries) {
            summary.setPredictors(new ArrayList<Predictor>());
            summary.setDetails(null);
        }
        return summaries;
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register a model summary")
    @PreAuthorize("hasRole('Create_PLS_Models')")
    public ModelSummary createModelSummary(@RequestBody ModelSummary modelSummary,
            @RequestParam(value = "raw", required = false) boolean usingRaw, HttpServletRequest request) {
        if (usingRaw) {
            modelSummary = modelSummaryParser.parse("", modelSummary.getRawFile());
            modelSummary.setUploaded(true);
        }

        Tenant tenant = SecurityUtils.getTenantFromRequest(request, globalSessionManagementService);
        if (tenant == null) { 
            return null; 
        }

        modelSummaryService.resolveNameIdConflict(modelSummary, tenant.getId());

        tenant = tenantEntityMgr.findByTenantId(tenant.getId());
        modelSummary.setTenant(tenant);

        modelSummaryEntityMgr.create(modelSummary);

        return modelSummary;
    }

    @RequestMapping(value = "/{modelId}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a model summary")
    @PreAuthorize("hasRole('Edit_PLS_Models')")
    public Boolean delete(@PathVariable String modelId) {
        modelSummaryEntityMgr.deleteByModelId(modelId);
        return true;
    }

    @RequestMapping(value = "/{modelId}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a model summary")
    @PreAuthorize("hasRole('Edit_PLS_Models')")
    public Boolean update(@PathVariable String modelId, @RequestBody AttributeMap attrMap) {
        ModelSummary modelSummary = new ModelSummary();
        modelSummary.setId(modelId);
        modelSummaryEntityMgr.updateModelSummary(modelSummary, attrMap);
        return true;
    }

    public void setModelSummaryEntityMgr(ModelSummaryEntityMgr modelSummaryEntityMgr) {
        this.modelSummaryEntityMgr = modelSummaryEntityMgr;
    }

    public ModelSummaryEntityMgr getModelSummaryEntityMgr() {
        return modelSummaryEntityMgr;
    }
}
