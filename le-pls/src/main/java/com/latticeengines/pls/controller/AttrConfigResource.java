package com.latticeengines.pls.controller;

import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

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
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.json.MappingJackson2JsonView;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionRequest;
import com.latticeengines.domain.exposed.pls.AttrConfigStateOverview;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
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
    public AttrConfigStateOverview getActivationOverview() {
        return attrConfigService.getOverallAttrConfigActivationOverview();
    }

    @GetMapping(value = "/usage/overview")
    @ResponseBody
    @ApiOperation("get usage overview")
    public AttrConfigUsageOverview getUsageOverview() {
        return attrConfigService.getOverallAttrConfigUsageOverview();
    }

    @PutMapping(value = "/activation/config/category/{categoryName}")
    @ApiOperation("update Activation Config")
    public void updateActivationConfig(@PathVariable String categoryName,
            @RequestBody AttrConfigSelectionRequest request) {
        attrConfigService.updateActivationConfig(categoryName, request);
    }

    @PutMapping(value = "/usage/config/category/{categoryName}")
    @ApiOperation("update Usage Config")
    public ModelAndView updateUsageConfig(@PathVariable String categoryName,
            @RequestParam(value = "usage", required = true) String usageName,
            @RequestBody AttrConfigSelectionRequest request, HttpServletResponse response) {
        MappingJackson2JsonView jsonView = new MappingJackson2JsonView();
        UIAction uiAction = attrConfigService.updateUsageConfig(categoryName, usageName, request);
        return new ModelAndView(jsonView, ImmutableMap.of(UIAction.class.getSimpleName(), uiAction));
    }

    @GetMapping(value = "/activation/config/category/{categoryName}")
    @ResponseBody
    @ApiOperation("get activation configuration detail for a specific category")
    public AttrConfigSelectionDetail getActivationConfiguration(@PathVariable String categoryName) {
        return attrConfigService.getAttrConfigSelectionDetailForState(categoryName);
    }

    @GetMapping(value = "/usage/config/category/{categoryName}")
    @ResponseBody
    @ApiOperation("get usage configuration detail for a specific category")
    public AttrConfigSelectionDetail getUsageConfiguration(@PathVariable String categoryName,
            @RequestParam(value = "usage", required = true) String usageName) {
        return attrConfigService.getAttrConfigSelectionDetails(categoryName, usageName);
    }

    @GetMapping(value = "/stats/category/{categoryName}")
    @ResponseBody
    @ApiOperation("get (attr, stats buckets) pairs for specific category and sub-category")
    public Map<String, AttributeStats> getStats(@PathVariable String categoryName,
            @RequestParam(value = "subcategory", required = true) String subcatName) {
        return attrConfigService.getStats(categoryName, subcatName);
    }
}
