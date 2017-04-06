package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.NameValidationUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelAlerts;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.service.ModelAlertService;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.security.exposed.util.SecurityUtils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelsummary", description = "REST resource for model summaries")
@RestController
@RequestMapping("/modelsummaries")
@PreAuthorize("hasRole('View_PLS_Models')")
public class ModelSummaryResource {

    private static final Logger log = Logger.getLogger(ModelSummaryResource.class);

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private SessionService sessionService;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private ModelAlertService modelAlertService;

    @Autowired
    private ModelMetadataService modelMetadataService;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private TenantService tenantService;

    @RequestMapping(value = "/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get summary for specific model")
    public ModelSummary getModelSummary(@PathVariable String modelId, HttpServletRequest request,
            HttpServletResponse response) {
        Tenant tenant = MultiTenantContext.getTenant();
        ModelSummary modelSummary = modelSummaryService.getModelSummary(modelId);

        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_18124, new String[] { modelId, tenant.getId() });
        }

        return modelSummary;
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of model summary ids available to the user")
    public List<ModelSummary> getModelSummaries(@RequestParam(value = "selection", required = false) String selection) {
        return modelSummaryService.getModelSummaries(selection);
    }

    @RequestMapping(value = "/updated/{timeframe}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a list of model summary updated within the timeframe as specified")
    public List<ModelSummary> getModelSummariesUpdatedWithinTimeFrame(@PathVariable long timeFrame) {
        return modelSummaryService.getModelSummariesModifiedWithinTimeFrame(timeFrame);
    }

    @RequestMapping(value = "/tenant/{tenantName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of model summaries available for given tenant")
    public List<ModelSummary> getAllForTenant(@PathVariable String tenantName) {
        Tenant tenant = tenantService.findByTenantName(tenantName);
        List<ModelSummary> summaries = modelSummaryEntityMgr.getAllByTenant(tenant);

        for (ModelSummary summary : summaries) {
            summary.setPredictors(new ArrayList<Predictor>());
            summary.setDetails(null);
        }
        return summaries;
    }

    @RequestMapping(value = "/alerts/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get diagnostic alerts for a model.")
    public String getModelAlerts(@PathVariable String modelId, HttpServletRequest request,
            HttpServletResponse response) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        String tenantId = tenant.getId();
        if (!modelSummaryService.modelIdinTenant(modelId, tenant.getId())) {
            response.setStatus(403);
            log.warn("Tenant " + tenant.getId() + " does not have the model " + modelId);
            return null;
        }

        ModelAlerts alerts = new ModelAlerts();
        ModelAlerts.ModelQualityWarnings modelQualityWarnings = modelAlertService.generateModelQualityWarnings(tenantId,
                modelId);
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
        if (!NameValidationUtils.validateModelName(modelId)) {
            log.error(String.format("Not qualified modelId %s contains unsupported characters.", modelId));
            return false;
        }
        modelSummaryService.updateModelSummary(modelId, attrMap);
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
    @ApiOperation(value = "Update predictors of a sourceModelSummary for the use of BuyerInsights")
    @PreAuthorize("hasRole('Edit_PLS_Models')")
    public Boolean updatePredictors(@PathVariable String modelId, @RequestBody AttributeMap attrMap) {
        modelSummaryService.updatePredictors(modelId, attrMap);
        return true;
    }

    @RequestMapping(value = "/metadata/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get metadata for the event table used for the specified model")
    @PreAuthorize("hasRole('View_PLS_Refine_Clone')")
    public List<VdbMetadataField> getMetadata(@PathVariable String modelId) {
        return modelMetadataService.getMetadata(modelId);
    }

    @RequestMapping(value = "/trainingdata/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get training table attributes used for the specified model")
    public ResponseDocument<List<Attribute>> getTableAttributes(@PathVariable String modelId) {
        ModelSummary modelSummary = modelSummaryEntityMgr.findValidByModelId(modelId);
        Table trainingTable = metadataProxy.getTable(MultiTenantContext.getCustomerSpace().toString(),
                modelSummary.getTrainingTableName());
        return ResponseDocument.successResponse(trainingTable.getAttributes());
    }

    @RequestMapping(value = "/metadata/required/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get required column names for the event table used for the specified model")
    public List<String> getRequiredColumns(@PathVariable String modelId) {
        return modelMetadataService.getRequiredColumnDisplayNames(modelId);
    }
}
