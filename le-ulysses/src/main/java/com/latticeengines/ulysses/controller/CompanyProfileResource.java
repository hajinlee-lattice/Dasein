package com.latticeengines.ulysses.controller;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.ulysses.CompanyProfile;
import com.latticeengines.ulysses.service.CompanyProfileService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "companyprofile", description = "REST resource for company profiles")
@RestController
@RequestMapping("/companyprofiles/")
public class CompanyProfileResource {

    @Autowired
    private CompanyProfileService companyProfileService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve a company profile")
    public CompanyProfile getCompanyProfile(@RequestBody Map<MatchKey, String> request) {
        return companyProfileService.getProfile(new CustomerSpace("A", "A", "Production"), request);
    }


}
