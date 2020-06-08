package com.latticeengines.pls.controller;

import java.util.List;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.NameValidationUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
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
import com.latticeengines.pls.service.ModelAlertService;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.util.SecurityUtils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelsummary", description = "REST resource for model summaries")
@RestController
@RequestMapping("/modelsummaries")
@PreAuthorize("hasRole('View_PLS_Models')")
public class ModelSummaryResource {

    private static final Logger log = LoggerFactory.getLogger(ModelSummaryResource.class);

    @Inject
    private SessionService sessionService;

    @Inject
    private ModelAlertService modelAlertService;

    @Inject
    private ModelMetadataService modelMetadataService;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @GetMapping("/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Get summary for specific model")
    public ModelSummary getModelSummary(@PathVariable String modelId, HttpServletRequest request,
            HttpServletResponse response) {
        Tenant tenant = MultiTenantContext.getTenant();
        ModelSummary modelSummary = modelSummaryProxy.getModelSummary(MultiTenantContext.getTenant().getId(), modelId);
        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_18124, new String[] { modelId, tenant.getId() });
        }

        return modelSummary;
    }

    @GetMapping("")
    @ResponseBody
    @ApiOperation(value = "Get list of model summary ids available to the user")
    public List<ModelSummary> getModelSummaries(@RequestParam(value = "selection", required = false) String selection) {
        return modelSummaryProxy.getModelSummaries(MultiTenantContext.getTenant().getId(), selection);
    }

    @GetMapping("/updated/{timeframe}")
    @ResponseBody
    @ApiOperation(value = "Get a list of model summary updated within the timeframe as specified")
    public List<ModelSummary> getModelSummariesUpdatedWithinTimeFrame(@PathVariable long timeFrame) {
        return modelSummaryProxy.getModelSummariesModifiedWithinTimeFrame(timeFrame);
    }

    @GetMapping("/tenant/{tenantName}")
    @ResponseBody
    @ApiOperation(value = "Get list of model summaries available for given tenant")
    public List<ModelSummary> getAllForTenant(@PathVariable String tenantName) {
        return modelSummaryProxy.getAllForTenant(tenantName);
    }

    @GetMapping("/alerts/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Get diagnostic alerts for a model.")
    public String getModelAlerts(@PathVariable String modelId, HttpServletRequest request,
            HttpServletResponse response) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        String tenantId = tenant.getId();
        if (!modelSummaryProxy.modelIdinTenant(tenant.getId(), modelId)) {
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

    @PostMapping("")
    @ResponseBody
    @ApiOperation(value = "Register a model summary")
    @PreAuthorize("hasRole('Create_PLS_Models')")
    public ModelSummary createModelSummary(@RequestBody ModelSummary modelSummary,
            @RequestParam(value = "raw", required = false) boolean usingRaw, HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        if (tenant == null) {
            return null;
        }

        return modelSummaryProxy.createModelSummary(tenant.getId(), modelSummary, usingRaw);
    }

    @DeleteMapping("/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Delete a model summary")
    @PreAuthorize("hasRole('Edit_PLS_Models')")
    public Boolean delete(@PathVariable String modelId) {
        modelSummaryProxy.deleteByModelId(MultiTenantContext.getTenant().getId(), modelId);

        return true;
    }

    @PutMapping("/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Update a model summary")
    @PreAuthorize("hasRole('Edit_PLS_Models')")
    public Boolean update(@PathVariable String modelId, @RequestBody AttributeMap attrMap) {
        if (!NameValidationUtils.validateModelName(modelId)) {
            log.error(String.format("Not qualified modelId %s contains unsupported characters.", modelId));
            return false;
        }
        modelSummaryProxy.update(MultiTenantContext.getTenant().getId(), modelId, attrMap);

        return true;
    }

    @GetMapping("/predictors/all/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Get all the predictors for a specific model")
    public List<Predictor> getAllPredictors(@PathVariable String modelId) {
        List<Predictor> predictors = modelSummaryProxy.getAllPredictors(MultiTenantContext.getTenant().getId(), modelId);

        return predictors;
    }

    @GetMapping("/predictors/bi/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Get predictors used by BuyerInsgihts for a specific model")
    public List<Predictor> getPredictorsForBuyerInsights(@PathVariable String modelId) {
        List<Predictor> predictors = modelSummaryProxy.getPredictorsForBuyerInsights(MultiTenantContext.getTenant().getId(), modelId);

        return predictors;
    }

    @PutMapping("/predictors/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Update predictors of a sourceModelSummary for the use of BuyerInsights")
    @PreAuthorize("hasRole('Edit_PLS_Models')")
    public Boolean updatePredictors(@PathVariable String modelId, @RequestBody AttributeMap attrMap) {
        modelSummaryProxy.updatePredictors(MultiTenantContext.getTenant().getId(), modelId, attrMap);

        return true;
    }

    @GetMapping("/metadata/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Get metadata for the event table used for the specified model")
    @PreAuthorize("hasRole('View_PLS_Refine_Clone')")
    public List<VdbMetadataField> getMetadata(@PathVariable String modelId) {
        return modelMetadataService.getMetadata(modelId);
    }

    @GetMapping("/trainingdata/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Get training table attributes used for the specified model")
    public ResponseDocument<List<Attribute>> getTableAttributes(@PathVariable String modelId) {
        ModelSummary modelSummary = modelSummaryProxy.findValidByModelId(MultiTenantContext.getTenant().getId(), modelId);
        Table trainingTable = metadataProxy.getTable(MultiTenantContext.getTenant().getId(),
                modelSummary.getTrainingTableName());

        return ResponseDocument.successResponse(trainingTable.getAttributes());
    }

    @GetMapping("/metadata/required/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Get required column names for the event table used for the specified model")
    public List<String> getRequiredColumns(@PathVariable String modelId) {
        return modelMetadataService.getRequiredColumnDisplayNames(modelId);
    }

    public ModelSummaryProxy getModelSummaryProxy() {
        return modelSummaryProxy;
    }
    public void setModelSummaryProxy(ModelSummaryProxy modelSummaryProxy) {
        this.modelSummaryProxy = modelSummaryProxy;
    }
}
