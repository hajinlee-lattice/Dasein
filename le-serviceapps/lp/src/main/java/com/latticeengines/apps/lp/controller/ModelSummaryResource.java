package com.latticeengines.apps.lp.controller;

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
import com.latticeengines.domain.exposed.pls.ModelSummary;

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

    @GetMapping("/getmodelsummarybymodelid/{modelSummaryId}")
    @ResponseBody
    @ApiOperation(value = "Get a model summary by the given momdel summary id")
    public ModelSummary getModelSummaryByModelId(@PathVariable String customerSpace, @PathVariable String modelSummaryId) {
        return modelSummaryService.getModelSummaryByModelId(modelSummaryId);
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

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of model summary ids available to the user")
    public List<ModelSummary> getModelSummaries(@PathVariable String customerSpace,
            @RequestParam(value = "selection", required = false) String selection) {
        return modelSummaryService.getModelSummaries(selection);
    }
}
