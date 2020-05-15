package com.latticeengines.pls.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.metadata.AttributeSet;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail.SubcategoryDetail;
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

    @GetMapping(value = "/usage/overview/attributeset")
    @ResponseBody
    @ApiOperation("get usage overview")
    public AttrConfigUsageOverview getUsageOverviewByAttributeSet(@RequestParam(value = "attributeSetName", required = false) String attributeSetName) {
            return attrConfigService.getOverallAttrConfigUsageOverview(attributeSetName);

    }

    @GetMapping(value = "/name/overview")
    @ResponseBody
    @ApiOperation("get Name overview")
    public AttrConfigStateOverview getNameOverview() {
        return attrConfigService.getOverallAttrConfigNameOverview();
    }

    @ResponseBody
    @PutMapping(value = "/activation/config/category/{categoryName}")
    @ApiOperation("update Activation Config")
    public Map<String, UIAction> updateActivationConfig(@PathVariable String categoryName,
            @RequestBody AttrConfigSelectionRequest request) {
        UIAction uiAction = attrConfigService.updateActivationConfig(categoryName, request);
        return ImmutableMap.of(UIAction.class.getSimpleName(), uiAction);
    }

    @PutMapping(value = "/usage/config/category/{categoryName}")
    @ApiOperation("update Usage Config")
    public Map<String, UIAction> updateUsageConfig(@PathVariable String categoryName,
            @RequestParam(value = "usage", required = true) String usageName,
            @RequestBody AttrConfigSelectionRequest request, HttpServletResponse response) {
        UIAction uiAction = attrConfigService.updateUsageConfig(categoryName, usageName, request);
        return ImmutableMap.of(UIAction.class.getSimpleName(), uiAction);
    }

    @PutMapping(value = "/name/config/category/{categoryName}")
    @ApiOperation("update Name or Description for Account/Contract attributes")
    public SubcategoryDetail updateNameConfig(@PathVariable String categoryName, @RequestBody SubcategoryDetail request,
            HttpServletResponse response) {
        return attrConfigService.updateNameConfig(categoryName, request);
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
                                                           @RequestParam(value = "usage") String usageName,
                                                           @RequestParam(value = "attributeSetName", required = false) String attributeSetName) {
        return attrConfigService.getAttrConfigSelectionDetailForUsage(categoryName, usageName, attributeSetName);
    }

    @GetMapping(value = "/name/config/category/{categoryName}")
    @ResponseBody
    @ApiOperation("get name configuration detail for a specific category")
    public SubcategoryDetail getNameConfiguration(@PathVariable String categoryName) {
        return attrConfigService.getAttrConfigSelectionDetailForName(categoryName);
    }

    @GetMapping(value = "/stats/category/{categoryName}")
    @ResponseBody
    @ApiOperation("get (attr, stats buckets) pairs for specific category and sub-category")
    public Map<String, AttributeStats> getStats(@PathVariable String categoryName,
            @RequestParam(value = "subcategory") String subcatName) {
        return attrConfigService.getStats(categoryName, subcatName);
    }

    @GetMapping(value = "/attributeset/name/{name}")
    @ApiOperation(value = "Get attribute set by name")
    public AttributeSet getAttributeSet(@PathVariable(value = "name") String name) {
        return attrConfigService.getAttributeSet(name);
    }

    @GetMapping(value = "/attributeset")
    @ApiOperation(value = "Get attribute set list")
    public List<AttributeSet> getAttributeSets() {
        return attrConfigService.getAttributeSets();
    }

    @DeleteMapping(value = "/attributeset/name/{name}")
    @ApiOperation(value = "Delete attribute set")
    public Boolean deleteAttributeSet(@PathVariable("name") String name) {
        attrConfigService.deleteAttributeSet(name);
        return true;
    }

    @PostMapping(value = "/attributeset/clone")
    @ApiOperation(value = "create new attribute set based on an existing attribute set")
    public AttributeSet cloneAttributeSet(@RequestParam(value = "attributeSetName") String attributeSetName,
                                          @RequestBody AttributeSet attributeSet) {
        return attrConfigService.cloneAttributeSet(attributeSetName, attributeSet);
    }

    @PostMapping(value = "/attributeset")
    @ApiOperation(value = "create new attribute set based on an existing attribute set")
    public AttributeSet createAttributeSet(@RequestBody AttributeSet attributeSet) {
        return attrConfigService.createAttributeSet(attributeSet);
    }

    @PutMapping(value = "/attributeset")
    @ApiOperation(value = "update attribute set")
    public AttributeSet updateAttributeSet(@RequestBody AttributeSet attributeSet) {
        return attrConfigService.updateAttributeSet(attributeSet);
    }
}
