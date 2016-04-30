package com.latticeengines.pls.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.network.exposed.pls.TargetMarketInterface;
import com.latticeengines.pls.service.TargetMarketService;
import com.latticeengines.pls.workflow.FitWorkflowSubmitter;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "targetmarket", description = "REST resource for target markets")
@RestController
@RequestMapping("/targetmarkets")
@PreAuthorize("hasRole('View_PLS_TargetMarkets')")
public class TargetMarketResource implements TargetMarketInterface {

    private static final String FIT_MODEL_WORKFLOW = "fitModelWorkflow";
    private static final String TEST_FIT_MODEL_WORKFLOW = "mockFitModelWorkflow";

    @Autowired
    private TargetMarketService targetMarketService;

    @Autowired
    private FitWorkflowSubmitter fitWorkflowSubmitter;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register a target market")
    @PreAuthorize("hasRole('Edit_PLS_TargetMarkets')")
    public void create(@RequestBody TargetMarket targetMarket) {
        if (targetMarketService.findTargetMarketByName(targetMarket.getName()) != null) {
            throw new RuntimeException(String.format("Target market %s already exists", targetMarket.getName()));
        }

        targetMarketService.createTargetMarket(targetMarket);
        fitWorkflowSubmitter.submitWorkflowForTargetMarketAndWorkflowName(targetMarket, FIT_MODEL_WORKFLOW);
    }

    @RequestMapping(value = "/default", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register a default target market")
    @PreAuthorize("hasRole('Edit_PLS_TargetMarkets')")
    public TargetMarket createDefault() {
        TargetMarket targetMarket = targetMarketService.createDefaultTargetMarket();
        fitWorkflowSubmitter.submitWorkflowForTargetMarketAndWorkflowName(targetMarket, FIT_MODEL_WORKFLOW);
        return targetMarket;
    }

    @RequestMapping(value = "/default/test", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register a default target market using the test fit model workflow")
    @PreAuthorize("hasRole('Edit_PLS_TargetMarkets')")
    public TargetMarket createDefaultTest() {
        TargetMarket targetMarket = targetMarketService.createDefaultTargetMarket();
        fitWorkflowSubmitter.submitWorkflowForTargetMarketAndWorkflowName(targetMarket, TEST_FIT_MODEL_WORKFLOW);
        return targetMarket;
    }

    @RequestMapping(value = "/default", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve the default target market")
    @PreAuthorize("hasRole('View_PLS_TargetMarkets')")
    public TargetMarket getDefault() {
        return targetMarketService.findTargetMarketByName(TargetMarket.DEFAULT_NAME);
    }

    @RequestMapping(value = "/{targetMarketName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a target market")
    @PreAuthorize("hasRole('Edit_PLS_TargetMarkets')")
    public void delete(@PathVariable String targetMarketName) {
        targetMarketService.deleteTargetMarketByName(targetMarketName);
    }

    @RequestMapping(value = "/{targetMarketName}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a target market")
    @PreAuthorize("hasRole('Edit_PLS_TargetMarkets')")
    public void update(@PathVariable String targetMarketName, @RequestBody TargetMarket targetMarket) {
        TargetMarket existing = targetMarketService.findTargetMarketByName(targetMarket.getName());

        targetMarketService.updateTargetMarketByName(targetMarket, targetMarketName);

        if (existing != null && !existing.getAccountFilterString().equals(targetMarket.getAccountFilterString())) {
            fitWorkflowSubmitter.submitWorkflowForTargetMarketAndWorkflowName(targetMarket, FIT_MODEL_WORKFLOW);
        }
    }

    @RequestMapping(value = "/{targetMarketName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a target market by a name")
    @PreAuthorize("hasRole('View_PLS_TargetMarkets')")
    public TargetMarket find(@PathVariable String targetMarketName) {
        return targetMarketService.findTargetMarketByName(targetMarketName);
    }

    @RequestMapping(value = "/{targetMarketName}/reports", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register a report")
    @Secured({ "Edit_PLS_TargetMarkets", "Edit_PLS_Reports" })
    public void registerReport(@PathVariable String targetMarketName, @RequestBody Report report) {
        targetMarketService.registerReport(targetMarketName, report);
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @PreAuthorize("hasRole('View_PLS_TargetMarkets')")
    public List<TargetMarket> findAll() {
        return targetMarketService.findAllTargetMarkets();
    }

    @RequestMapping(value = "/default/reset", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Reset a default target market")
    @PreAuthorize("hasRole('Edit_PLS_TargetMarkets')")
    public Boolean resetDefaultTargetMarket() {
        return targetMarketService.resetDefaultTargetMarket();
    }
}
