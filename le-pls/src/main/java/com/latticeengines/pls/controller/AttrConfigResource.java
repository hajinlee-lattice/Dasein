package com.latticeengines.pls.controller;

import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.AttrConfigActivationOverview;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionRequest;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;
import com.latticeengines.pls.service.AttrConfigService;

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
    public Map<String, AttrConfigActivationOverview> getActivationOverview() {
        return attrConfigService.getOverallAttrConfigActivationOverview();
    }

    @GetMapping(value = "/usage/overview")
    @ResponseBody
    @ApiOperation("get usage overview")
    public AttrConfigUsageOverview getUsageOverview() {
        return attrConfigService.getOverallAttrConfigUsageOverview();
    }

    @PutMapping(value = "/activation/config/category/{categoryName}")
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation("update Activation Config")
    public void updateActivationConfig(@PathVariable String categoryName,
            @RequestBody AttrConfigSelectionRequest request) {
        attrConfigService.updateActivationConfig(categoryName, request);
    }

    @PutMapping(value = "/usage/config/category/{categoryName}")
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation("update Usage Config")
    public void updateUsageConfig(@PathVariable String categoryName,
            @RequestParam(value = "usage", required = true) String usage,
            @RequestBody AttrConfigSelectionRequest request) {
        attrConfigService.updateUsageConfig(categoryName, usage, request);
    }

    @GetMapping(value = "/activation/config/category/{categoryName}")
    @ResponseBody
    @ApiOperation("get activation configuration detail for a specific category")
    public AttrConfigSelectionDetail getActivationConfiguration(@PathVariable String categoryName) {
        log.info("get activation configuration detail for " + categoryName);
        return attrConfigService.getAttrConfigSelectionDetailForState(categoryName);
    }

    @GetMapping(value = "/usage/config/category/{categoryName}")
    @ResponseBody
    @ApiOperation("get usage configuration detail for a specific category")
    public AttrConfigSelectionDetail getUsageConfiguration(@PathVariable String categoryName,
            @RequestParam(value = "usage", required = true) String usage) {
        log.info("get usage configuration detail for " + categoryName);
        return attrConfigService.getAttrConfigSelectionDetails(categoryName, usage);
    }

}
