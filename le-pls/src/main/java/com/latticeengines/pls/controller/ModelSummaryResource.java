package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "modelsummary", description = "REST resource for model summaries")
@RestController
@RequestMapping("/modelsummaries")
@PreAuthorize("hasRole('View_PLS_Models')")
public class ModelSummaryResource {
    
    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;
    
    @RequestMapping(value = "/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get summary for specific model")
    public ModelSummary getModelSummary(@PathVariable Long modelId) {
        ModelSummary s = new ModelSummary();
        s.setPid(modelId);
        return modelSummaryEntityMgr.findByKey(s);
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of model summary ids available to the user")
    public List<ModelSummary> getModelSummaries() {
        List<ModelSummary> summaries = modelSummaryEntityMgr.findAll();
        
        for (ModelSummary summary : summaries) {
            summary.setPredictors(new ArrayList<Predictor>());
        }
        return summaries;
    }

}
