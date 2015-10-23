package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
import com.latticeengines.domain.exposed.pls.ModelAlerts;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.service.ModelAlertService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.security.exposed.service.SessionService;
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
    private SessionService sessionService;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private ModelAlertService modelAlertService;

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
    public List<ModelSummary> getModelSummaries(@RequestParam(value = "selection", required = false) String selection) {

        List<ModelSummary> summaries;
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

    @RequestMapping(value = "/alerts/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get diagnostic alerts for a model.")
    public String getModelAlerts(@PathVariable String modelId, HttpServletRequest request, HttpServletResponse response) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        String tenantId = tenant.getId();
        if (!modelSummaryService.modelIdinTenant(modelId, tenant.getId())) {
            response.setStatus(403);
            return null;
        }

        ModelAlerts alerts = new ModelAlerts();
        ModelAlerts.ModelQualityWarnings modelQualityWarnings = modelAlertService.generateModelQualityWarnings(
                tenantId, modelId);
        ModelAlerts.MissingMetaDataWarnings missingMetaDataWarnings = modelAlertService
                .generateMissingMetaDataWarnings(tenantId, modelId);
        alerts.setMissingMetaDataWarnings(missingMetaDataWarnings);
        alerts.setModelQualityWarnings(modelQualityWarnings);

        return alerts.toString();
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register a model summary")
    @PreAuthorize("hasRole('Create_PLS_Models')")
    public ModelSummary createModelSummary(@RequestBody ModelSummary modelSummary,
            @RequestParam(value = "raw", required = false) boolean usingRaw, HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        if (tenant == null) {
            return null;
        }

        if (usingRaw) {
            return modelSummaryService.createModelSummary(modelSummary.getRawFile(), tenant.getId());
        } else {
            return modelSummaryService.createModelSummary(modelSummary, tenant.getId());
        }
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

    @RequestMapping(value = "/predictors/all/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all the predictors for a specific model")
    public List<Predictor> getAllPredictors(@PathVariable String modelId) {
        List<Predictor> predictors = modelSummaryEntityMgr.findAllPredictorsByModelId(modelId);
        return predictors;
    }

    @RequestMapping(value = "/predictors/bi/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get predictors used by BuyerInsgihts for a specific model")
    public List<Predictor> getPredictorsForBuyerInsights(@PathVariable String modelId) {
        List<Predictor> predictors = modelSummaryEntityMgr.findPredictorsUsedByBuyerInsightsByModelId(modelId);
        return predictors;
    }

    @RequestMapping(value = "/predictors/{modelId}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update predictors of a modelSummary for the use of BuyerInsights")
    @PreAuthorize("hasRole('Edit_PLS_Models')")
    public Boolean updatePredictors(@PathVariable String modelId, @RequestBody AttributeMap attrMap) {
        modelSummaryService.updatePredictors(modelId, attrMap);
        return true;
    }
}
