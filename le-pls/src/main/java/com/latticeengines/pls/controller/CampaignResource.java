package com.latticeengines.pls.controller;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.ulysses.Campaign;
import com.latticeengines.pls.service.CampaignService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "campaign", description = "REST resource for campaigns")
@RestController
@RequestMapping("/campaigns")
@PreAuthorize("hasRole('View_PLS_Campaigns')")
public class CampaignResource {

    @Autowired
    private CampaignService campaignService;

    @RequestMapping(value = "/{campaignName}/models/", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a campaign from a list of models")
    @PreAuthorize("hasRole('Edit_PLS_Campaigns')")
    public SimpleBooleanResponse createFromModel(@PathVariable String campaignName, //
            @RequestBody List<String> modelIds, //
            HttpServletRequest request) {
        campaignService.createCampaignFromModels(campaignName, null, modelIds, request);
        return SimpleBooleanResponse.successResponse();
    }

    @RequestMapping(value = "/{campaignName}/tables/{tableName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a campaign from a list")
    @PreAuthorize("hasRole('Edit_PLS_Campaigns')")
    public SimpleBooleanResponse createFromTable(@PathVariable String campaignName, //
            @PathVariable String tableName, //
            HttpServletRequest request) {
        return SimpleBooleanResponse.successResponse();
    }

    @RequestMapping(value = "/{campaignName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a campaign by a name")
    public Campaign findCampaignByName(@PathVariable String campaignName) {
        return campaignService.findCampaignByName(campaignName);
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all campaigns")
    public List<Campaign> findAll() {
        return campaignService.findAll();
    }

    @RequestMapping(value = "/{campaignName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a campaign by a name")
    @PreAuthorize("hasRole('Edit_PLS_Campaigns')")
    public SimpleBooleanResponse deleteCampaignByName(@PathVariable String campaignName) {
        campaignService.deleteCampaignByName(campaignName);
        return SimpleBooleanResponse.successResponse();
    }

}
