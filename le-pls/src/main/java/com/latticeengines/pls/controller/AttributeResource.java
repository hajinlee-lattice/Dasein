package com.latticeengines.pls.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.app.exposed.service.AttributeCustomizationService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "attributes", description = "REST resource for attributes")
@RestController
@RequestMapping("/attributes")
public class AttributeResource {

    @Autowired
    private AttributeCustomizationService attributeCustomizationService;

    @RequestMapping(value = "/flags/{name}/{useCase}/{propertyName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Save attribute property")
    public void savePropertyValue(@PathVariable String name, @PathVariable AttributeUseCase useCase,
            @PathVariable String propertyName, @RequestBody String value) {
        attributeCustomizationService.save(name, useCase, propertyName, value);
    }

    @RequestMapping(value = "/flags/{name}/{useCase}/{propertyName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get attribute property")
    public String getPropertyValue(@PathVariable String name, @PathVariable AttributeUseCase useCase,
            @PathVariable String propertyName) {
        String value = attributeCustomizationService.retrieve(name, useCase, propertyName);
        return JsonUtils.serialize(ImmutableMap.<String, String> of("value", value));
    }

    @RequestMapping(value = "/categories/flags/{categoryName}/{useCase}/{propertyName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Save attribute property")
    public void savePropertyValueBaseOnCategory(@PathVariable Category category,
            @PathVariable AttributeUseCase useCase, @PathVariable String propertyName, @RequestBody String value) {
        attributeCustomizationService.saveCategory(category, useCase, propertyName, value);
    }

    @RequestMapping(value = "/categories/subcategories/flags/{categoryName}/{subcategoryName}/{useCase}/{propertyName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Save attribute property")
    public void savePropertyValueBaseOnSubCategory(@PathVariable Category category,
            @PathVariable String subcategoryName, @PathVariable AttributeUseCase useCase,
            @PathVariable String propertyName, @RequestBody String value) {
        attributeCustomizationService.saveSubCategory(category, subcategoryName, useCase, propertyName,
                value);
    }

}
