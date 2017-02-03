package com.latticeengines.pls.controller;

import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.CompanyProfileService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.ulysses.CompanyProfile;
import com.latticeengines.security.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "companyprofiles", description = "REST resource for company profiles")
@RestController
@RequestMapping("/companyprofiles/")
@PreAuthorize("hasRole('View_PLS_Data')")
public class CompanyProfileResource {

    private static final Logger log = Logger.getLogger(CompanyProfileResource.class);

    @Autowired
    private CompanyProfileService companyProfileService;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve a company profile")
    public CompanyProfile getCompanyProfile( //
            @RequestBody Map<String, String> attributes, //
            @RequestParam(value = "enforceFuzzyMatch", required = false, defaultValue = "true")//
            boolean enforceFuzzyMatch) {
        CustomerSpace space = MultiTenantContext.getCustomerSpace();
        log.info(String.format("Retrieving company profile for %s, attributes = [%s]", space, attributes));
        return companyProfileService.getProfile(space, attributes, enforceFuzzyMatch);
    }
}