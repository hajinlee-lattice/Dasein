package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringUtils;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.LeadEnrichmentService;
import com.latticeengines.pls.service.SelectedAttrService;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.util.SecurityUtils;
import com.wordnik.swagger.annotations.ApiParam;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "leadenrichment", description = "REST resource for lead enrichment")
@RestController
@RequestMapping(value = "/leadenrichment")
@PreAuthorize("hasRole('Edit_PLS_Configurations')")
public class LeadEnrichmentResource {

    public static final String LP2_ENRICH_PATH = "";
    public static final String LP3_ENRICH_PATH = "/v3";

    @Autowired
    private SessionService sessionService;

    @Autowired
    private LeadEnrichmentService leadEnrichmentService;

    @Autowired
    private SelectedAttrService selectedAttrService;

    // ------------START for LP v2-------------------//
    @RequestMapping(value = LP2_ENRICH_PATH
            + "/avariableattributes", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all avariable attributes")
    public List<LeadEnrichmentAttribute> getAvariableAttributes(HttpServletRequest request) {
        return leadEnrichmentService.getAvailableAttributes();
    }

    @RequestMapping(value = LP2_ENRICH_PATH
            + "/attributes", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get saved attributes")
    public List<LeadEnrichmentAttribute> getAttributes(HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        return leadEnrichmentService.getAttributes(tenant);
    }

    @RequestMapping(value = LP2_ENRICH_PATH
            + "/verifyattributes", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Verify attributes and return fields not exist in target system")
    public Map<String, List<String>> verifyAttributes(@RequestBody List<LeadEnrichmentAttribute> attributes,
            HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        return leadEnrichmentService.verifyAttributes(tenant, attributes);
    }

    @RequestMapping(value = LP2_ENRICH_PATH
            + "/attributes", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Save attributes")
    public Boolean saveAttributes(@RequestBody List<LeadEnrichmentAttribute> attributes, HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        leadEnrichmentService.saveAttributes(tenant, attributes);
        return true;
    }

    @RequestMapping(value = LP2_ENRICH_PATH
            + "/templatetype", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get template type")
    public String getTemplateType(HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        String type = leadEnrichmentService.getTemplateType(tenant);
        return JsonUtils.serialize(type);
    }

    @RequestMapping(value = LP2_ENRICH_PATH
            + "/premiumattributeslimitation", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get premium attributes limitation")
    public Integer getPremiumAttributesLimitation(HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        return leadEnrichmentService.getPremiumAttributesLimitation(tenant);
    }
    // ------------END for LP v2-------------------//

    // ------------START for LP v3-------------------//
    @RequestMapping(value = LP3_ENRICH_PATH + "/categories", method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of categories")
    public List<String> getLP3Categories(HttpServletRequest request) {
        List<String> categoryStrList = new ArrayList<>();
        for (Category category : Category.values()) {
            categoryStrList.add(category.toString());
        }
        return categoryStrList;
    }

    @RequestMapping(value = LP3_ENRICH_PATH, //
            method = RequestMethod.PUT, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Save attributes")
    public Boolean saveLP3Attributes(HttpServletRequest request, //
            @RequestBody LeadEnrichmentAttributesOperationMap attributes) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        selectedAttrService.save(attributes, tenant, getPremiumAttributesLimitation(request));

        return true;
    }

    @RequestMapping(value = LP3_ENRICH_PATH, //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of attributes with selection flag")
    public List<LeadEnrichmentAttribute> getLP3Attributes(HttpServletRequest request,
            @ApiParam(value = "Get attributes with name containing specified " //
                    + "text for attributeDisplayNameFilter", required = false) //
            @RequestParam(value = "attributeDisplayNameFilter", required = false) //
            String attributeDisplayNameFilter, //
            @ApiParam(value = "Get attributes " //
                    + "with specified category", required = false) //
            @RequestParam(value = "category", required = false) //
            String category, //
            @ApiParam(value = "Should get only selected attribute", //
                    required = false) //
            @RequestParam(value = "onlySelectedAttributes", required = false) //
            Boolean onlySelectedAttributes) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        Category categoryEnum = (StringUtils.objectIsNullOrEmptyString(category) ? null : Category.fromName(category));
        return selectedAttrService.getAttributes(tenant, attributeDisplayNameFilter, categoryEnum,
                onlySelectedAttributes);
    }

    @RequestMapping(value = LP3_ENRICH_PATH + "/premiumattributeslimitation", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get premium attributes limitation")
    public Integer getLP3PremiumAttributesLimitation(HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        return selectedAttrService.getPremiumAttributesLimitation(tenant);
    }

    @RequestMapping(value = LP3_ENRICH_PATH + "/selectedattributes/count", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get selected attributes count")
    public Integer getLP3SelectedAttributeCount(HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        return selectedAttrService.getSelectedAttributeCount(tenant);
    }

    @RequestMapping(value = LP3_ENRICH_PATH + "/selectedpremiumattributes/count", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get selected premium attributes count")
    public Integer getLP3SelectedAttributePremiumCount(HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        return selectedAttrService.getSelectedAttributePremiumCount(tenant);
    }
    // ------------END for LP v3-------------------//
}
