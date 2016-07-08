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

import com.latticeengines.common.exposed.util.StringUtils;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.SelectedAttrService;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.util.SecurityUtils;
import com.wordnik.swagger.annotations.ApiParam;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "enrichment", description = "REST resource for configuring enrichment")
@RestController
@RequestMapping(value = "/enrichment")
@PreAuthorize("hasRole('Edit_PLS_Configurations')")
public class EnrichmentResource {
    public static final String LEAD_ENRICH_PATH = "/lead";

    @Autowired
    private SessionService sessionService;

    @Autowired
    private SelectedAttrService selectedAttrService;

    // ------------START for LeadEnrichment-------------------//
    @RequestMapping(value = LEAD_ENRICH_PATH + "/categories", method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of categories")
    public List<String> getLeadEnrichmentCategories(HttpServletRequest request) {
        List<String> categoryStrList = new ArrayList<>();
        for (Category category : Category.values()) {
            categoryStrList.add(category.toString());
        }
        return categoryStrList;
    }

    @RequestMapping(value = LEAD_ENRICH_PATH, //
            method = RequestMethod.PUT, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Save attributes")
    public void saveLeadEnrichmentAttributes(HttpServletRequest request, //
            @RequestBody LeadEnrichmentAttributesOperationMap attributes) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        selectedAttrService.save(attributes, tenant, getLeadEnrichmentPremiumAttributesLimitation(request));
    }

    @RequestMapping(value = LEAD_ENRICH_PATH, //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of attributes with selection flag")
    public List<LeadEnrichmentAttribute> getLeadEnrichmentAttributes(HttpServletRequest request,
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

    @RequestMapping(value = LEAD_ENRICH_PATH + "/premiumattributeslimitation", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get premium attributes limitation")
    public Map<String, Integer> getLeadEnrichmentPremiumAttributesLimitation(HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        return selectedAttrService.getPremiumAttributesLimitation(tenant);
    }

    @RequestMapping(value = LEAD_ENRICH_PATH + "/selectedattributes/count", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get selected attributes count")
    public Integer getLeadEnrichmentSelectedAttributeCount(HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        return selectedAttrService.getSelectedAttributeCount(tenant);
    }

    @RequestMapping(value = LEAD_ENRICH_PATH + "/selectedpremiumattributes/count", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get selected premium attributes count")
    public Integer getLeadEnrichmentSelectedAttributePremiumCount(HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        return selectedAttrService.getSelectedAttributePremiumCount(tenant);
    }
    // ------------END for LeadEnrichment-------------------//
}
