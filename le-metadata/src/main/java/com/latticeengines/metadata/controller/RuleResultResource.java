package com.latticeengines.metadata.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.ModelReviewData;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
import com.latticeengines.metadata.service.ModelReviewService;
import com.latticeengines.metadata.service.RuleResultService;

@Api(value = "metadata", description = "REST resource for rule result artifacts")
@RestController
@RequestMapping("/ruleresults")
public class RuleResultResource {

    @Autowired
    private ModelReviewService modelReviewService;

    @Autowired
    private RuleResultService ruleResultService;

    @RequestMapping(value = "/column", //
    method = RequestMethod.POST, //
    headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create column rule results")
    public Boolean createColumnResults(@RequestBody List<ColumnRuleResult> columnResults, //
            HttpServletRequest request) {
        ruleResultService.createColumnResults(columnResults);
        return true;
    }

    @RequestMapping(value = "/row", //
    method = RequestMethod.POST, //
    headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create row rule results")
    public Boolean createRowResults(@RequestBody List<RowRuleResult> rowResults, //
            HttpServletRequest request) {
        ruleResultService.createRowResults(rowResults);
        return true;
    }

    @RequestMapping(value = "/column/{modelId}", //
    method = RequestMethod.GET, //
    headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of column results")
    public List<ColumnRuleResult> getColumnResults(@PathVariable String modelId, //
            HttpServletRequest request) {
        return ruleResultService.findColumnResults(modelId);
    }

    @RequestMapping(value = "/row/{modelId}", //
    method = RequestMethod.GET, //
    headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of row results")
    public List<RowRuleResult> getRowResults(@PathVariable String modelId, //
            HttpServletRequest request) {
        return ruleResultService.findRowResults(modelId);
    }

    @RequestMapping(value = "/reviewdata/{modelId}/{eventTableName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get model review data for a model")
    public ModelReviewData getReviewData(@PathVariable String modelId, @PathVariable String eventTableName) {
        return modelReviewService.getReviewData(modelId, eventTableName);
    }
}
