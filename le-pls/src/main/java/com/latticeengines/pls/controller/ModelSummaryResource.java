package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
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
    public ModelSummary getModelSummary(@PathVariable String modelId) {
        ModelSummary summary = modelSummaryEntityMgr.findByModelId(modelId);
        if (summary != null) {
            summary.setPredictors(new ArrayList<Predictor>());
        }
        return summary;
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of model summary ids available to the user")
    public List<ModelSummary> getModelSummaries() {
        List<ModelSummary> summaries = modelSummaryEntityMgr.findAllValid();

        for (ModelSummary summary : summaries) {
            summary.setPredictors(new ArrayList<Predictor>());
            summary.setDetails(null);
        }
        return summaries;
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
        if (attrMap.containsKey("Status")) {
            updateModelStatus(modelId, attrMap);
        } else {
            ModelSummary modelSummary = new ModelSummary();
            modelSummary.setId(modelId);
            modelSummary.setName(attrMap.get("Name"));
            modelSummaryEntityMgr.updateModelSummary(modelSummary);
        }
        return true;
    }
    
    private void updateModelStatus(String modelId, AttributeMap attrMap) {
        String status = attrMap.get("Status");
        switch (status) {
        case "UpdateAsDeleted":
            modelSummaryEntityMgr.updateStatusByModelId(modelId, ModelSummaryStatus.DELETED);
            break;
        case "UpdateAsInactive":
            modelSummaryEntityMgr.updateStatusByModelId(modelId, ModelSummaryStatus.INACTIVE);
            break;
        case "UpdateAsActive":
            modelSummaryEntityMgr.updateStatusByModelId(modelId, ModelSummaryStatus.ACTIVE);
            break;
        default:
            break;
        }
    }

    public void setModelSummaryEntityMgr(ModelSummaryEntityMgr modelSummaryEntityMgr) {
        this.modelSummaryEntityMgr = modelSummaryEntityMgr;
    }
    
}
