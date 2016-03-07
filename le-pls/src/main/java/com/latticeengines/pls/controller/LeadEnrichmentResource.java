package com.latticeengines.pls.controller;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.LeadEnrichmentService;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.util.SecurityUtils;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "leadenrichment", description = "REST resource for lead enrichment")
@RestController
@RequestMapping(value = "/leadenrichment")
@PreAuthorize("hasRole('Edit_PLS_Configurations')")
public class LeadEnrichmentResource {

    @Autowired
    private SessionService sessionService;

    @Autowired
    private LeadEnrichmentService leadEnrichmentService;

    @RequestMapping(value = "/avariableattributes", method=RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all avariable attributes")
    public List<LeadEnrichmentAttribute> getAvariableAttributes(HttpServletRequest request) {
        return leadEnrichmentService.getAvailableAttributes();
    }

    @RequestMapping(value = "/attributes", method=RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get saved attributes")
    public List<LeadEnrichmentAttribute> getAttributes(HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        return leadEnrichmentService.getAttributes(tenant);
    }

    @RequestMapping(value = "/verifyattributes", method=RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Verify attributes and return fields not exist in target system")
    public Map<String, List<String>> verifyAttributes(@RequestBody List<LeadEnrichmentAttribute> attributes, HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        return leadEnrichmentService.verifyAttributes(tenant, attributes);
    }

    @RequestMapping(value = "/attributes", method=RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Save attributes")
    public Boolean saveAttributes(@RequestBody List<LeadEnrichmentAttribute> attributes, HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        leadEnrichmentService.saveAttributes(tenant, attributes);
        return true;
    }

    @RequestMapping(value = "/templatetype", method=RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get template type")
    public String getTemplateType(HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        String type = leadEnrichmentService.getTemplateType(tenant);
        return JsonUtils.serialize(type);
    }

    @RequestMapping(value = "/premiumattributeslimitation", method=RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get premium attributes limitation")
    public Integer getPremiumAttributesLimitation(HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        return leadEnrichmentService.getPremiumAttributesLimitation(tenant);
    }
}
