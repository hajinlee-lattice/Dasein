package com.latticeengines.ulysses.controller;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.ulysses.PrimaryFieldConfiguration;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.ulysses.service.AttributeService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "attributes", description = "REST resource for attribute configurations")
@RestController
@RequestMapping("/attributes")
@ResponseBody
public class AttributeResource {

    @Autowired
    private AttributeService attributeService;

    @RequestMapping(value = "/primaryfield-configuration", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Provides all matching attributes and its validation expression that are required for Scoring, Company Lookup API using Global Field Mappings")
    public PrimaryFieldConfiguration getPrimaryAttributeConfiguration(HttpServletRequest request) {
        PrimaryFieldConfiguration primaryConfig = new PrimaryFieldConfiguration();
        primaryConfig.setPrimaryFields(attributeService.getPrimaryFields());

        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        primaryConfig.setValidationExpression(attributeService.getPrimaryFieldValidationExpression(customerSpace));
        return primaryConfig;
    }
}
