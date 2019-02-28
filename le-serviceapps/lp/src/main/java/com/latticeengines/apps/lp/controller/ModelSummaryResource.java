package com.latticeengines.apps.lp.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.lp.entitymgr.ModelSummaryDownloadFlagEntityMgr;
import com.latticeengines.apps.lp.service.ModelSummaryService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.Predictor;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "model summaries", description = "REST resource for model summaries")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/modelsummaries")
public class ModelSummaryResource {

    private static final Logger log = LoggerFactory.getLogger(ModelSummaryResource.class);

    @Inject
    private ModelSummaryDownloadFlagEntityMgr downloadFlagEntityMgr;

    @Inject
    private ModelSummaryService modelSummaryService;

    @PostMapping("/downloadflag")
    @ResponseBody
    @ApiOperation(value = "Set model summary download flag")
    public void setDownloadFlag(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        log.info(String.format("Set model summary download flag for tenant %s", customerSpace));
        downloadFlagEntityMgr.addDownloadFlag(customerSpace);
    }

    @PostMapping("/downloadmodelsummary")
    @ResponseBody
    @ApiOperation(value = "Download model summary")
    public Boolean downloadModelSummary(@PathVariable String customerSpace,
            @RequestBody(required = false) Map<String, String> modelApplicationIdToEventColumn) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        try {
            return modelSummaryService.downloadModelSummary(customerSpace, modelApplicationIdToEventColumn);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    @PostMapping("/geteventtomodelsummary")
    @ResponseBody
    @ApiOperation(value = "Get event to model summary")
    public Map<String, ModelSummary> getEventToModelSummary(@PathVariable String customerSpace,
            @RequestBody Map<String, String> modelApplicationIdToEventColumn) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return modelSummaryService.getEventToModelSummary(customerSpace, modelApplicationIdToEventColumn);
    }


    @RequestMapping(value = "/create", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create model summary")
    public void createModelSummary(@PathVariable String customerSpace, @RequestBody ModelSummary modelSummary) {
        modelSummaryService.create(modelSummary);
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register a model summary")
    public ModelSummary createModelSummary(@PathVariable String customerSpace, @RequestBody ModelSummary modelSummary,
                                           @RequestParam(value = "raw", required = false) boolean usingRaw) {
        ModelSummary summary;
        if (usingRaw) {
            summary = modelSummaryService.createModelSummary(modelSummary.getRawFile(), customerSpace);
        } else {
            summary = modelSummaryService.createModelSummary(modelSummary, customerSpace);
        }
        clearPredictorFroModelSummary(summary);

        return summary;
    }

    @RequestMapping(value = "/{modelId}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a model summary")
    public Boolean update(@PathVariable String customerSpace, @PathVariable String modelId, @RequestBody AttributeMap attrMap) {
        modelSummaryService.updateModelSummary(modelId, attrMap);
        return true;
    }

    @RequestMapping(value = "/updatestatus/{modelId}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update model summary by model id")
    public Boolean updateStatusByModelId(@PathVariable String customerSpace,
                                         @PathVariable String modelId, @RequestBody ModelSummaryStatus status) {
        modelSummaryService.updateStatusByModelId(modelId, status);
        return true;
    }

    @RequestMapping(value = "/{modelId}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a model summary")
    public Boolean deleteByModelId(@PathVariable String customerSpace, @PathVariable String modelId) {
        modelSummaryService.deleteByModelId(modelId);
        return true;
    }

    @GetMapping("/{modelId}")
    @ResponseBody
    @ApiOperation(value = "get model summary")
    public ModelSummary getModelSummary(@PathVariable String customerSpace, @PathVariable String modelId) {
        ModelSummary modelSummary = modelSummaryService.getModelSummary(modelId);
        clearPredictorFroModelSummary(modelSummary);

        return modelSummary;
    }

    @RequestMapping(value = "/findbymodelid/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Find model summary by model id")
    public ModelSummary findByModelId(@PathVariable String customerSpace, @PathVariable String modelId,
                                      @RequestParam(value = "relational", required = false) boolean returnRelational,
                                      @RequestParam(value = "document", required = false) boolean returnDocument,
                                      @RequestParam(value = "validonly", required = false) boolean validOnly) {
        ModelSummary modelSummary = modelSummaryService.findByModelId(modelId, returnRelational, returnDocument, validOnly);
        clearPredictorFroModelSummary(modelSummary);

        return modelSummary;
    }

    @GetMapping("/findvalidbymodelId/{modelSummaryId}")
    @ResponseBody
    @ApiOperation(value = "Get a valid model summary by the given model summary id")
    public ModelSummary findValidByModelId(@PathVariable String customerSpace, @PathVariable String modelSummaryId) {
        ModelSummary modelSummary = modelSummaryService.findValidByModelId(modelSummaryId);
        clearPredictorFroModelSummary(modelSummary);

        return modelSummary;
    }

    @GetMapping("/getmodelsummaryenrichedbydetails/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Get model summary enriched by details")
    public ModelSummary getModelSummaryEnrichedByDetails(@PathVariable String customerSpace, @PathVariable String modelId) {
        ModelSummary modelSummary = modelSummaryService.getModelSummaryEnrichedByDetails(modelId);
        clearPredictorFroModelSummary(modelSummary);

        return modelSummary;
    }

    @GetMapping("/findbyapplicationid/{applicationId}")
    @ResponseBody
    @ApiOperation(value = "Find a model summary by application id")
    public ModelSummary findByApplicationId(@PathVariable String customerSpace, @PathVariable String applicationId) {
        ModelSummary modelSummary = modelSummaryService.findByApplicationId(applicationId);
        clearPredictorFroModelSummary(modelSummary);

        return modelSummary;
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of model summary ids available to the user")
    public List<ModelSummary> getModelSummaries(@PathVariable String customerSpace,
                                                @RequestParam(value = "selection", required = false) String selection) {
        return modelSummaryService.getModelSummaries(selection);
    }

    @RequestMapping(value = "/findall", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Find all model summaries")
    public List<ModelSummary> findAll(@PathVariable String customerSpace) {
        List<ModelSummary> modelSummaries = modelSummaryService.getAll();
        clearPredictorFroModelSummaries(modelSummaries);

        return modelSummaries;
    }

    @RequestMapping(value = "/findallvalid", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Find total count")
    public List<ModelSummary> findAllValid(@PathVariable String customerSpace) {
        List<ModelSummary> modelSummaries = modelSummaryService.findAllValid();
        clearPredictorFroModelSummaries(modelSummaries);

        return modelSummaries;
    }

    @RequestMapping(value = "/findallactive", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Find total count")
    public List<ModelSummary> findAllActive(@PathVariable String customerSpace) {
        List<ModelSummary> modelSummaries =  modelSummaryService.findAllActive();
        clearPredictorFroModelSummaries(modelSummaries);

        return modelSummaries;
    }

    @RequestMapping(value = "/findpaginatedmodels", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Find paginated models")
    public List<ModelSummary> findPaginatedModels(@PathVariable String customerSpace, @RequestParam long lastUpdateTime,
            @RequestParam boolean considerAllStatus, @RequestParam int offset, @RequestParam int maximum) {
        List<ModelSummary> modelSummaries = modelSummaryService.findPaginatedModels(lastUpdateTime, considerAllStatus, offset, maximum);
        clearPredictorFroModelSummaries(modelSummaries);

        return modelSummaries;
    }

    @RequestMapping(value = "/findtotalcount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Find total count")
    public int findTotalCount(@PathVariable String customerSpace, @RequestParam long lastUpdateTime, @RequestParam boolean considerAllStatus) {
        return modelSummaryService.findTotalCount(lastUpdateTime, considerAllStatus);
    }

    @RequestMapping(value = "/alerts/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get diagnostic alerts for a model")
    public Boolean modelIdinTenant(@PathVariable String customerSpace, @PathVariable String modelId) {
        return modelSummaryService.modelIdinTenant(modelId, customerSpace);
    }

    @RequestMapping(value = "/predictors/all/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all the predictors for a specific model")
    public List<Predictor> getAllPredictors(@PathVariable String customerSpace, @PathVariable String modelId) {
        List<Predictor> predictors = modelSummaryService.findAllPredictorsByModelId(modelId);
        return predictors;
    }

    @RequestMapping(value = "/predictors/bi/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get predictors used by BuyerInsgihts for a specific model")
    public List<Predictor> getPredictorsForBuyerInsights(@PathVariable String customerSpace, @PathVariable String modelId) {
        List<Predictor> predictors = modelSummaryService.findPredictorsUsedByBuyerInsightsByModelId(modelId);
        return predictors;
    }

    @RequestMapping(value = "/predictors/{modelId}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update predictors of a sourceModelSummary for the use of BuyerInsights")
    public Boolean updatePredictors(@PathVariable String customerSpace,
                                    @PathVariable String modelId, @RequestBody AttributeMap attrMap) {
        modelSummaryService.updatePredictors(modelId, attrMap);
        return true;
    }

    private void clearPredictorFroModelSummary (ModelSummary modelSummary) {
        if (modelSummary != null) {
            modelSummary.setPredictors(new ArrayList<>());
        }
    }

    private void clearPredictorFroModelSummaries (List<ModelSummary> modelSummaries) {
        if (modelSummaries != null) {
            for (ModelSummary modelSummary : modelSummaries) {
                modelSummary.setPredictors(new ArrayList<>());
                modelSummary.setDetails(null);
            }
        }
    }
}
