package com.latticeengines.apps.lp.controller;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.apps.lp.service.ModelNoteService;
import com.latticeengines.apps.lp.service.ModelSummaryService;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.service.TenantService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelsummary_internal", description = "REST resource for model summary")
@RestController
@RequestMapping("/modelsummaries/internal")
public class ModelSummaryInternalResource {

    private static final Logger log = LoggerFactory.getLogger(ModelSummaryInternalResource.class);

    @Inject
    private ModelSummaryService modelSummaryService;

    @Inject
    private TenantService tenantService;

    @Inject
    private ModelNoteService modelNoteService;

    @GetMapping("/getmodelsummarybymodelid/{modelSummaryId}")
    @ResponseBody
    @NoCustomerSpace
    @ApiOperation(value = "Get a model summary by the given model summary id")
    public ModelSummary getModelSummaryByModelId(@PathVariable String modelSummaryId) {
        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelSummaryId);
        clearPredictorFroModelSummary(modelSummary);

        return modelSummary;
    }

    @GetMapping("/retrievebymodelidforinternaloperations/{modelId}")
    @ResponseBody
    @NoCustomerSpace
    @ApiOperation(value = "Retrieve by model id for internal operations")
    public ModelSummary retrieveByModelIdForInternalOperations(@PathVariable String modelId) {
        ModelSummary modelSummary = modelSummaryService.retrieveByModelIdForInternalOperations(modelId);
        clearPredictorFroModelSummary(modelSummary);

        return modelSummary;
    }

    @GetMapping("/tenant/{tenantName}")
    @ResponseBody
    @NoCustomerSpace
    @ApiOperation(value = "Get list of model summaries available for given tenant")
    public List<ModelSummary> getAllForTenant(@PathVariable String tenantName) {
        Tenant tenant = tenantService.findByTenantName(tenantName);
        List<ModelSummary> modelSummaries = modelSummaryService.getAllByTenant(tenant);
        clearPredictorFroModelSummaries(modelSummaries);

        return modelSummaries;
    }

    @GetMapping("/getmodelsummariesbyapplicationid/{applicationId}")
    @ResponseBody
    @NoCustomerSpace
    @ApiOperation(value = "Find a list of model summaries by application id")
    public List<ModelSummary> getModelSummariesByApplicationId(@PathVariable String applicationId) {
        List<ModelSummary> modelSummaries = modelSummaryService.getModelSummariesByApplicationId(applicationId);
        clearPredictorFroModelSummaries(modelSummaries);

        return modelSummaries;
    }

    @GetMapping("/updated/{timeframe}")
    @ResponseBody
    @NoCustomerSpace
    @ApiOperation(value = "get all data feeds.")
    public List<ModelSummary> getModelSummariesUpdatedWithinTimeFrame(@PathVariable long timeframe) {
        List<ModelSummary> modelSummaries = modelSummaryService.getModelSummariesModifiedWithinTimeFrame(timeframe);
        clearPredictorFroModelSummaries(modelSummaries);

        return modelSummaries;
    }

    @DeleteMapping("/modelnote/{modelSummaryId}")
    @NoCustomerSpace
    @ApiOperation(value = "delete model note by id.")
    public void deleteById(@PathVariable String modelSummaryId) {
        modelNoteService.deleteById(modelSummaryId);
    }

    @PutMapping("/modelnote/{modelSummaryId}")
    @NoCustomerSpace
    @ApiOperation(value = "update model note by id.")
    public void updateById(@PathVariable String modelSummaryId, @RequestBody NoteParams noteParams) {
        modelNoteService.updateById(modelSummaryId, noteParams);
    }

    @PostMapping("/modelnote/{modelSummaryId}")
    @NoCustomerSpace
    @ApiOperation(value = "create model note by id.")
    public void create(@PathVariable String modelSummaryId, @RequestBody NoteParams noteParams) {
        modelNoteService.create(modelSummaryId, noteParams);
    }

    @PostMapping("/modelnote/copy/{sourceModelSummaryId}")
    @NoCustomerSpace
    @ApiOperation(value = "copy model notes.")
    public void copyNotes(@PathVariable String sourceModelSummaryId, @RequestParam(value = "targetModelSummaryId") String targetModelSummaryId) {
        modelNoteService.copyNotes(sourceModelSummaryId, targetModelSummaryId);
    }

    private void clearPredictorFroModelSummary (ModelSummary modelSummary) {
        if (modelSummary != null) {
            modelSummary.setPredictors(new ArrayList<Predictor>());
        }
    }

    private void clearPredictorFroModelSummaries (List<ModelSummary> modelSummaries) {
        if (modelSummaries != null) {
            for (ModelSummary modelSummary : modelSummaries) {
                modelSummary.setPredictors(new ArrayList<Predictor>());
                modelSummary.setDetails(null);
            }
        }
    }
}
