package com.latticeengines.metadata.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.ModelReviewData;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
import com.latticeengines.metadata.service.ModelReviewService;
import com.latticeengines.metadata.service.RuleResultService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for rule result artifacts")
@RestController
@RequestMapping("/ruleresults")
public class RuleResultResource {

    @Inject
    private ModelReviewService modelReviewService;

    @Inject
    private RuleResultService ruleResultService;

    @PostMapping("/column")
    @ResponseBody
    @ApiOperation(value = "Create column rule results")
    public Boolean createColumnResults(@RequestBody List<ColumnRuleResult> columnResults) {
        ruleResultService.createColumnResults(columnResults);
        return true;
    }

    @PostMapping("/row")
    @ResponseBody
    @ApiOperation(value = "Create row rule results")
    public Boolean createRowResults(@RequestBody List<RowRuleResult> rowResults) {
        ruleResultService.createRowResults(rowResults);
        return true;
    }

    @GetMapping("/column/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Get list of column results")
    public List<ColumnRuleResult> getColumnResults(@PathVariable String modelId) {
        return ruleResultService.findColumnResults(modelId);
    }

    @GetMapping("/row/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Get list of row results")
    public List<RowRuleResult> getRowResults(@PathVariable String modelId) {
        return ruleResultService.findRowResults(modelId);
    }

    @GetMapping("/reviewdata/{customerSpace}/{modelId}/{eventTableName}")
    @ResponseBody
    @ApiOperation(value = "Get model review data for a model")
    public ModelReviewData getReviewData(@PathVariable String customerSpace, @PathVariable String modelId, @PathVariable String eventTableName) {
        return modelReviewService.getReviewData(customerSpace, modelId, eventTableName);
    }
}
