package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.AttrConfigActivationOverview;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
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
    public List<AttrConfigActivationOverview> getActivationOverview() {
        List<AttrConfigActivationOverview> list = new ArrayList<>();
        for (Category category : Category.getPremiunCategories()) {
            list.add(attrConfigService.getAttrConfigActivationOverview(category));
        }
        return list;
    }

    @GetMapping(value = "/usage/overview")
    @ResponseBody
    @ApiOperation("get usage overview")
    public AttrConfigUsageOverview getUsageOverview() {
        AttrConfigUsageOverview usageOverview = attrConfigService.getAttrConfigUsageOverview();
        return usageOverview;
    }

    @GetMapping(value = "/activation/config/category/{categoryName}")
    @ResponseBody
    @ApiOperation("get activation configuration for a specific category")
    public AttrConfigRequest getActivationConfiguration(@PathVariable String categoryName) {
        AttrConfigRequest request = new AttrConfigRequest();
        return request;
    }

}
