package com.latticeengines.apps.lp.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.lp.entitymgr.ModelSummaryDownloadFlagEntityMgr;
import com.latticeengines.apps.lp.entitymgr.ModelSummaryEntityMgr;
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
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @PostMapping("/downloadflag")
    @ResponseBody
    @ApiOperation(value = "Set model summary download flag")
    public void setDownloadFlag(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        log.info(String.format("Set model summary download flag for tenant %s", customerSpace));
        downloadFlagEntityMgr.addDownloadFlag(customerSpace);
    }

    @GetMapping("/{modelSummaryId}")
    @ResponseBody
    @ApiOperation(value = "Get a model summary by the given momdel summary id")
    public ModelSummary setDownloadFlag(@PathVariable String customerSpace, @PathVariable String modelSummaryId) {
        return modelSummaryEntityMgr.getByModelId(modelSummaryId);
    }

}
