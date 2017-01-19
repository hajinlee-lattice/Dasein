package com.latticeengines.pls.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.AttributeCustomizationService;
import com.latticeengines.domain.exposed.pls.AttributeFlags;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "attributes", description = "REST resource for attributes")
@RestController
@RequestMapping("/attributes")
public class AttributeResource {

    @Autowired
    private AttributeCustomizationService attributeCustomizationService;

    @RequestMapping(value = "/flags/{name}/{useCase}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Save attribute flags")
    public void saveAttributeFlags(@PathVariable String name, @PathVariable AttributeUseCase useCase,
            @RequestBody AttributeFlags flags) {
        attributeCustomizationService.save(name, useCase, flags);
    }

    @RequestMapping(value = "/flags/{name}/{useCase}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get attribute flags")
    public AttributeFlags getAttributeFlags(@PathVariable String name, @PathVariable AttributeUseCase useCase) {
        return attributeCustomizationService.retrieve(name, useCase);
    }

}
