package com.latticeengines.pls.controller;

import java.util.Map;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
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

    @Inject
    private AttributeCustomizationService attributeCustomizationService;

    @PostMapping("/flags/{name}/{useCase}")
    @ResponseBody
    @ApiOperation(value = "Save attribute property")
    public void savePropertyValues(@PathVariable String name, @PathVariable AttributeUseCase useCase,
            @RequestBody Map<String, String> properties) {
        attributeCustomizationService.save(name, useCase, properties);
    }

    @PostMapping("/flags/{name}/{useCase}/{propertyName}")
    @ResponseBody
    @ApiOperation(value = "Save attribute property")
    public void savePropertyValue(@PathVariable String name, @PathVariable AttributeUseCase useCase,
            @PathVariable String propertyName, @RequestBody String value) {
        attributeCustomizationService.save(name, useCase, propertyName, value);
    }

    @GetMapping("/flags/{name}/{useCase}/{propertyName}")
    @ResponseBody
    @ApiOperation(value = "Get attribute property")
    public String getPropertyValue(@PathVariable String name, @PathVariable AttributeUseCase useCase,
            @PathVariable String propertyName) {
        String value = attributeCustomizationService.retrieve(name, useCase, propertyName);
        return JsonUtils.serialize(ImmutableMap.<String, String> of("value", value));
    }

    @PostMapping("/categories/flags/{useCase}/{propertyName}")
    @ResponseBody
    @ApiOperation(value = "Save attribute property")
    public void saveCategory(@RequestParam("category") String categoryName, @PathVariable AttributeUseCase useCase,
            @PathVariable String propertyName, @RequestBody String value) {
        attributeCustomizationService.saveCategory(Category.fromName(categoryName), useCase, propertyName, value);
    }

    @PostMapping("/categories/subcategories/flags/{useCase}/{propertyName}")
    @ResponseBody
    @ApiOperation(value = "Save attribute property")
    public void saveSubcategory(@RequestParam("category") String categoryName, @RequestParam("subcategory") String subcategoryName,
            @PathVariable AttributeUseCase useCase, @PathVariable String propertyName, @RequestBody String value) {
        attributeCustomizationService.saveSubcategory(Category.fromName(categoryName), subcategoryName, useCase,
                propertyName, value);
    }

    @PostMapping("/categories/flags/{useCase}")
    @ResponseBody
    @ApiOperation(value = "Save attribute property")
    public void saveCategoryProperties(@RequestParam("category") String categoryName, @PathVariable AttributeUseCase useCase,
            @RequestBody Map<String, String> properties) {
        attributeCustomizationService.saveCategory(Category.fromName(categoryName), useCase, properties);
    }

    @PostMapping("/categories/subcategories/flags/{useCase}")
    @ResponseBody
    @ApiOperation(value = "Save attribute property")
    public void saveSubcategoryProperties(@RequestParam("category") String categoryName, @RequestParam("subcategory") String subcategoryName,
            @PathVariable AttributeUseCase useCase, @RequestBody Map<String, String> properties) {
        attributeCustomizationService.saveSubcategory(Category.fromName(categoryName), subcategoryName, useCase,
                properties);
    }
}
