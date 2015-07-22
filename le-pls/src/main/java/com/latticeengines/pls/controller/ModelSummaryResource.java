package com.latticeengines.pls.controller;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
    public List<ModelSummary> getModelSummaries(
            @RequestParam(value = "selection", required = false) String selection) {

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
    public ModelAlerts getModelAlerts(@PathVariable String modelId, HttpServletRequest request,
            HttpServletResponse response) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        if (!modelSummaryService.modelIdinTenant(modelId, tenant.getId())) {
            response.setStatus(403);
            return null;
        }

        // TODO: to be replaced by true model alerts
        ModelAlerts mockAlerts = new ModelAlerts();
        ModelAlerts.ModelQualityWarnings modelQualityWarnings = mockAlerts.new ModelQualityWarnings();
        modelQualityWarnings.setLowSuccessEvents(437L);
        modelQualityWarnings.setMinSuccessEvents(500L);
        modelQualityWarnings.setLowConversionPercentage(0.8);
        modelQualityWarnings.setMinConversionPercentage(1.0);
        modelQualityWarnings.setOutOfRangeRocScore(0.62);
        modelQualityWarnings.setMinRocScore(0.7);
        modelQualityWarnings.setMaxRocScore(0.85);
        List<Map.Entry<String, String>> excessiveDiscreteValuesAttributes = new ArrayList<>();
        excessiveDiscreteValuesAttributes.add(new AbstractMap.SimpleEntry<String, String>(
                "attribuite1", "220"));
        excessiveDiscreteValuesAttributes.add(new AbstractMap.SimpleEntry<String, String>(
                "attribuite2", "389"));
        modelQualityWarnings.setExcessiveDiscreteValuesAttributes(excessiveDiscreteValuesAttributes);
        modelQualityWarnings.setMaxNumberOfDiscreteValues(200L);
        List<Map.Entry<String, String>> excessivePredictiveAttributes = new ArrayList<Map.Entry<String, String>>();
        excessivePredictiveAttributes.add(new AbstractMap.SimpleEntry<String, String>("attribuite1",
                "0.12"));
        excessivePredictiveAttributes.add(new AbstractMap.SimpleEntry<String, String>("attribuite2",
                "0.25"));
        modelQualityWarnings.setExcessivePredictiveAttributes(excessivePredictiveAttributes);
        modelQualityWarnings.setMaxFeatureImportance(1.0);
        List<Map.Entry<String, String>> excessivePredictiveNullValuesAttributes = new ArrayList<Map.Entry<String, String>>();
        excessivePredictiveNullValuesAttributes.add(new AbstractMap.SimpleEntry<String, String>(
                "attribuite1", "1.3"));
        excessivePredictiveNullValuesAttributes.add(new AbstractMap.SimpleEntry<String, String>(
                "attribuite2", "1.7"));
        modelQualityWarnings
                .setExcessivePredictiveNullValuesAttributes(excessivePredictiveNullValuesAttributes);
        modelQualityWarnings.setMaxLiftForNull(1.0);

        ModelAlerts.MissingMetaDataWarnings missingMetaDataWarnings = mockAlerts.new MissingMetaDataWarnings();
        List<String> approvedUsageValuesMissingAttributes = Arrays.asList("attribuite1",
                "attribuite2");
        missingMetaDataWarnings
                .setInvalidApprovedUsageMissingAttributes(approvedUsageValuesMissingAttributes);
        List<String> tagsValuesMissingAttributes = Arrays.asList("attribuite1", "attribuite2");
        missingMetaDataWarnings.setInvalidTagsAttributes(tagsValuesMissingAttributes);
        List<String> categoryValuesMissingAttributes = Arrays.asList("attribuite1", "attribuite2");
        missingMetaDataWarnings.setInvalidCategoryAttributes(categoryValuesMissingAttributes);
        List<String> displayNameValuesMissingAttributes = Arrays.asList("attribuite1",
                "attribuite2");
        missingMetaDataWarnings
                .setInvalidDisplayNameAttributes(displayNameValuesMissingAttributes);
        List<String> statisticalTypeValuesMissingAttributes = Arrays.asList("attribuite1",
                "attribuite2");
        missingMetaDataWarnings
                .setInvalidStatisticalTypeAttributes(statisticalTypeValuesMissingAttributes);
        List<String> excessiveCategoriesInModelSummary = Arrays.asList("category1", "category2");
        missingMetaDataWarnings.setExcessiveCategoriesInModelSummary(excessiveCategoriesInModelSummary);
        missingMetaDataWarnings.setMaxCategoriesInModelSummary(8L);

        mockAlerts.setModelQualityWarnings(modelQualityWarnings);
        mockAlerts.setMissingMetaDataWarnings(missingMetaDataWarnings);

        return mockAlerts;
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register a model summary")
    @PreAuthorize("hasRole('Create_PLS_Models')")
    public ModelSummary createModelSummary(@RequestBody ModelSummary modelSummary,
            @RequestParam(value = "raw", required = false) boolean usingRaw,
            HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        if (tenant == null) {
            return null;
        }

        if (usingRaw) {
            return modelSummaryService
                    .createModelSummary(modelSummary.getRawFile(), tenant.getId());
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
}
