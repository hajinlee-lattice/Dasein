package com.latticeengines.pls.controller;

import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionRequest;
import com.latticeengines.domain.exposed.pls.AttrConfigStateOverview;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;
import com.latticeengines.pls.service.AttrConfigService;
import com.latticeengines.pls.service.impl.AttrConfigServiceImpl.UpdateUsageResponse;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "AttrConfig", description = "REST resource for attribute config.")
@RestController
@RequestMapping(value = "/attrconfig")
public class AttrConfigResource {

    private static final Logger log = LoggerFactory.getLogger(AttrConfigResource.class);

    @Inject
    private AttrConfigService attrConfigService;

    @GetMapping(value = "/activation/overview")
    @ResponseBody
    @ApiOperation("get activation overview")
    public AttrConfigStateOverview getActivationOverview() {
        return attrConfigService.getOverallAttrConfigActivationOverview();
    }

    @GetMapping(value = "/usage/overview")
    @ResponseBody
    @ApiOperation("get usage overview")
    public AttrConfigUsageOverview getUsageOverview() {
        return attrConfigService.getOverallAttrConfigUsageOverview();
    }

    @PutMapping(value = "/activation/config/category/{categoryDisplayName}")
    @ApiOperation("update Activation Config")
    public void updateActivationConfig(@PathVariable String categoryDisplayName,
            @RequestBody AttrConfigSelectionRequest request) {
        attrConfigService.updateActivationConfig(categoryDisplayName, request);
    }

    @PutMapping(value = "/usage/config/category/{categoryDisplayName}")
    @ApiOperation("update Usage Config")
    public String updateUsageConfig(@PathVariable String categoryDisplayName,
            @RequestParam(value = "usage", required = true) String usageName,
            @RequestBody AttrConfigSelectionRequest request, HttpServletResponse response) {
        UpdateUsageResponse updateUsageResponse = attrConfigService.updateUsageConfig(categoryDisplayName, usageName,
                request);
        if (updateUsageResponse.getMessage() != null) {
            response.setStatus(HttpStatus.ORDINAL_500_Internal_Server_Error);
        }
        return updateUsageResponse.getMessage();
    }

    @GetMapping(value = "/activation/config/category/{categoryDisplayName}")
    @ResponseBody
    @ApiOperation("get activation configuration detail for a specific category")
    public AttrConfigSelectionDetail getActivationConfiguration(@PathVariable String categoryDisplayName) {
        return attrConfigService.getAttrConfigSelectionDetailForState(categoryDisplayName);
    }

    @GetMapping(value = "/usage/config/category/{categoryDisplayName}")
    @ResponseBody
    @ApiOperation("get usage configuration detail for a specific category")
    public AttrConfigSelectionDetail getUsageConfiguration(@PathVariable String categoryDisplayName,
            @RequestParam(value = "usage", required = true) String usageName) {
        return attrConfigService.getAttrConfigSelectionDetails(categoryDisplayName, usageName);
    }

    @GetMapping(value = "/stats/category/{catDisplayName}/subcategory/{subcatName}")
    @ResponseBody
    @ApiOperation("get (attr, stats buckets) pairs for specific category and sub-category")
    public Map<String, AttributeStats> getStats(@PathVariable String catDisplayName, @PathVariable String subcatName) {
        return attrConfigService.getStats(catDisplayName, subcatName);
    }
}
