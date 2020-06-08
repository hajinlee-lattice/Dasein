package com.latticeengines.apps.lp.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.lp.service.ModelFeatureImportanceService;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.ModelFeatureImportance;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "featureImportance", description = "REST resource for Feature Importance")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/featureimportances")
public class ModelFeatureImportanceResource {

    @Inject
    private ModelFeatureImportanceService importanceService;

    @PostMapping("/model/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Upsert feature importances")
    public SimpleBooleanResponse upsertFeatureImportances(@PathVariable String customerSpace,
            @PathVariable String modelId) {
        importanceService.upsertFeatureImportances(customerSpace, modelId);
        return SimpleBooleanResponse.successResponse();
    }

    @GetMapping("/model/{modelGuid}")
    @ResponseBody
    @ApiOperation(value = "Get model's feature importances by model GUID")
    public List<ModelFeatureImportance> getFeatureImportancesByModelGuid(@PathVariable String customerSpace,
            @PathVariable String modelGuid) {
        return importanceService.getFeatureImportances(customerSpace, modelGuid);
    }

}
