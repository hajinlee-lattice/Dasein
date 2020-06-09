package com.latticeengines.app.exposed.controller;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.GetMapping;

import com.latticeengines.app.exposed.service.PrimaryAttributeService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.ulysses.PrimaryFieldConfiguration;

import io.swagger.annotations.ApiOperation;

public class PrimaryAttributeResourceBase {

    public PrimaryAttributeResourceBase() {
        super();
    }

    @Inject
    private PrimaryAttributeService attributeService;

    @GetMapping("/primaryfield-configuration")
    @ApiOperation(value = "Provides all matching attributes and its validation expression that are required for Scoring, Company Lookup API using Global Field Mappings")
    public PrimaryFieldConfiguration getPrimaryAttributeConfiguration(HttpServletRequest request) {
            PrimaryFieldConfiguration primaryConfig = new  PrimaryFieldConfiguration();
            primaryConfig.setPrimaryFields(attributeService.getPrimaryFields());

            CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
            primaryConfig.setValidationExpression(attributeService.getPrimaryFieldValidationExpression(customerSpace));
            return primaryConfig;
    }
}
