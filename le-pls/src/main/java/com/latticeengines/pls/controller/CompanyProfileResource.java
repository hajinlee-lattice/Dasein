package com.latticeengines.pls.controller;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
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

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve a company profile")
    public CompanyProfile getCompanyProfile( //
            HttpServletRequest request, //
            @RequestParam(value = "enforceFuzzyMatch", required = false, defaultValue = "true")//
            boolean enforceFuzzyMatch) {
        CustomerSpace space = MultiTenantContext.getCustomerSpace();
        Map<String, String> attributes = getAccountAttributes(request);
        log.info(String.format("Retrieving company profile for %s, attributes = [%s]", space, attributes));
        return companyProfileService.getProfile(space, attributes, enforceFuzzyMatch);
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> getAccountAttributes(HttpServletRequest request) {
        Enumeration<String> parameterNames = request.getParameterNames();
        Map<String, String> parameters = new HashMap<>();

        while (parameterNames.hasMoreElements()) {
            String parameterName = parameterNames.nextElement();
            parameters.put(parameterName, request.getParameter(parameterName));
        }
        return parameters;
    }

}