package com.latticeengines.apps.core.controller;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigCategoryOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "AttrConfig", description = "REST resource for default LP attribute config.")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/attrconfig")
public class AttrConfigResource {

    @Inject
    private AttrConfigService attrConfigService;

    @GetMapping(value = "/entities/{entity}")
    @ResponseBody
    @ApiOperation("get cdl attribute config request")
    public AttrConfigRequest getAttrConfigByEntity(@PathVariable String customerSpace,
            @PathVariable BusinessEntity entity,
            @RequestParam(value = "render", required = false, defaultValue = "true") boolean render) {
        AttrConfigRequest request = new AttrConfigRequest();
        List<AttrConfig> attrConfigs = attrConfigService.getRenderedList(entity, render);
        request.setAttrConfigs(attrConfigs);
        return request;
    }

    @GetMapping(value = "/categories/{categoryName}")
    @ResponseBody
    @ApiOperation("get cdl attribute config request")
    public AttrConfigRequest getAttrConfigByCategory(@PathVariable String customerSpace,
            @PathVariable String categoryName) {
        AttrConfigRequest request = new AttrConfigRequest();
        Category category = resolveCategory(categoryName);
        List<AttrConfig> attrConfigs = attrConfigService.getRenderedList(category);
        request.setAttrConfigs(attrConfigs);
        return request;
    }

    @Deprecated
    @GetMapping(value = "")
    @ResponseBody
    @ApiOperation("get cdl attribute management overview")
    public List<AttrConfigOverview<?>> getAttrConfigOverview(@PathVariable String customerSpace,
            @RequestParam(value = "category", required = false) String categoryName, //
            @RequestParam(value = "property", required = true) String propertyName) {
        Category category = categoryName != null ? resolveCategory(categoryName) : null;
        List<AttrConfigOverview<?>> list = attrConfigService.getAttrConfigOverview(category, propertyName);
        return list;
    }

    @PostMapping(value = "/overview")
    public Map<String, AttrConfigCategoryOverview<?>> getAttrConfigOverview(@PathVariable String customerSpace,
            @RequestParam(value = "category", required = false) List<String> categoryNames, //
            @RequestParam(value = "activeOnly", required = false, defaultValue = "0") boolean activeOnly, //
            @RequestBody List<String> propertyNames) {
        List<Category> categories = categoryNames != null
                ? categoryNames.stream().map(this::resolveCategory).collect(Collectors.toList())
                : Arrays.asList(Category.values()).stream().filter(category -> !category.isHiddenFromUi())
                        .collect(Collectors.toList());
        return attrConfigService.getAttrConfigOverview(categories, propertyNames, activeOnly);
    }

    @PostMapping(value = "")
    @ResponseBody
    @ApiOperation("save cdl attribute config request")
    public AttrConfigRequest saveAttrConfig(@PathVariable String customerSpace,
            @RequestBody AttrConfigRequest request) {
        request.fixJsonDeserialization();
        return attrConfigService.saveRequest(request);
    }

    @PostMapping(value = "/validate")
    @ResponseBody
    @ApiOperation("put cdl attribute config request")
    public AttrConfigRequest validateAttrConfig(@PathVariable String customerSpace,
            @RequestBody AttrConfigRequest request) {
        request.fixJsonDeserialization();
        return attrConfigService.validateRequest(request);
    }

    private Category resolveCategory(String categoryName) {
        Category category = Category.fromName(categoryName);
        if (category == null) {
            throw new IllegalArgumentException("Cannot parse category " + categoryName);
        }
        return category;
    }

}
