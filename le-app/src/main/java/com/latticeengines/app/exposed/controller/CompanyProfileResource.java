package com.latticeengines.app.exposed.controller;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.CompanyProfileService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.ulysses.CompanyProfile;
import com.latticeengines.domain.exposed.ulysses.CompanyProfileRequest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "companyprofiles", description = "REST resource for company profiles")
@RestController
@RequestMapping("/companyprofiles/")
public class CompanyProfileResource {

    private static final Logger log = LoggerFactory.getLogger(CompanyProfileResource.class);

    @Inject
    private CompanyProfileService companyProfileService;

    @PostMapping("")
    @ResponseBody
    @ApiOperation(value = "Retrieve a company profile")
    public CompanyProfile getCompanyProfile( //
            @RequestBody CompanyProfileRequest request, //
            @RequestParam(value = "enforceFuzzyMatch", required = false, defaultValue = "true")//
            boolean enforceFuzzyMatch) {
        CustomerSpace space = MultiTenantContext.getCustomerSpace();
        log.info(String.format("Retrieving company profile for %s, request = [%s]", space, request));
        validateRequest(request);
        return companyProfileService.getProfile(space, request, enforceFuzzyMatch);
    }

    private void validateRequest(CompanyProfileRequest request) {
        if (MapUtils.isEmpty(request.getRecord())) {
            throw new LedpException(LedpCode.LEDP_18198);
        }
    }
}
