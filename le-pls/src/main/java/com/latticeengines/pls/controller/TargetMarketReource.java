package com.latticeengines.pls.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.pls.service.TargetMarketService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "targetmarket", description = "REST resource for target market")
@RestController
@RequestMapping("/targetmarket")
@PreAuthorize("hasRole('View_PLS_TargetMarkets')")
public class TargetMarketReource {

    @Autowired
    private TargetMarketService targetMarketService;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register a target market")
    @PreAuthorize("hasRole('Create_PLS_TargetMarkets')")
    public void create(@RequestBody TargetMarket targetMarket) {
        this.targetMarketService.createTargetMarket(targetMarket);
    }

    @RequestMapping(value = "/{targetMarketName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a target market")
    @PreAuthorize("hasRole('Edit_PLS_TargetMarkets')")
    public void delete(@PathVariable String targetMarketName) {
        this.targetMarketService.deleteTargetMarketByName(targetMarketName);
    }

    @RequestMapping(value = "/{targetMarketName}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a target market")
    @PreAuthorize("hasRole('Edit_PLS_TargetMarkets')")
    public void update(@PathVariable String targetMarketName,
            @RequestBody TargetMarket targetMarket) {
        this.targetMarketService.updateTargetMarketByName(targetMarket,
                targetMarketName);
    }

    @RequestMapping(value = "/{targetMarketName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a target market by a name")
    @PreAuthorize("hasRole('View_PLS_TargetMarkets')")
    public TargetMarket find(@PathVariable String targetMarketName) {
        return this.targetMarketService.getTargetMarketByName(targetMarketName);
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    public List<TargetMarket> findAll() {
        return this.targetMarketService.getAllTargetMarkets();
    }
}
