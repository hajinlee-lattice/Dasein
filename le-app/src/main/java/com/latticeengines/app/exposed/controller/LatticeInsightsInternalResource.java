package com.latticeengines.app.exposed.controller;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.AttributeService;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.TicketAuthenticationToken;
import com.latticeengines.security.exposed.service.TenantService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "latticeinsights", description = "REST resource for Lattice Insights")
@RestController
@RequestMapping("/internal/latticeinsights")
public class LatticeInsightsInternalResource {

    private static final Logger log = LoggerFactory.getLogger(LatticeInsightsInternalResource.class);

    @Inject
    private TenantService tenantService;

    @Inject
    private AttributeService attributeService;

    public static final String TENANT_ID_PATH = "{tenantId:\\w+\\.\\w+\\.\\w+}";

    @GetMapping("/enrichment" + LatticeInsightsResource.INSIGHTS_PATH + "/categories/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Get list of categories")
    public List<String> getLeadEnrichmentCategories(@PathVariable("tenantId") String tenantId) {
        List<LeadEnrichmentAttribute> allAttributes = getLeadEnrichmentAttributes(tenantId, null, null, null,
                false, null, null, Boolean.FALSE);
        List<String> categoryStrList = new ArrayList<>();
        for (Category category : Category.values()) {
            if (containsAtLeastOneAttributeForCategory(allAttributes, category)) {
                categoryStrList.add(category.toString());
            }
        }
        return categoryStrList;
    }

    @GetMapping("/enrichment" + LatticeInsightsResource.INSIGHTS_PATH + "/subcategories" + "/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Get list of subcategories for a given category")
    public List<String> getLeadEnrichmentSubcategories(@PathVariable("tenantId") String tenantId, //
                                                       @ApiParam(value = "category", required = true) //
                                                       @RequestParam String category) {
        Set<String> subcategories = new HashSet<>();
        List<LeadEnrichmentAttribute> allAttributes = getLeadEnrichmentAttributes(tenantId, null, category,
                null, false, null, null, Boolean.FALSE);
        for (LeadEnrichmentAttribute attr : allAttributes) {
            subcategories.add(attr.getSubcategory());
        }
        return new ArrayList<>(subcategories);
    }

    @GetMapping("/enrichment" + LatticeInsightsResource.INSIGHTS_PATH + "/" + TENANT_ID_PATH)
    @ResponseBody
    @ApiOperation(value = "Get lead enrichment")
    public List<LeadEnrichmentAttribute> getLeadEnrichmentAttributes(@PathVariable("tenantId") String tenantId,
            @ApiParam(value = "Get attributes with display name containing specified text (case insensitive) for attributeDisplayNameFilter")
            @RequestParam(value = "attributeDisplayNameFilter", required = false) String attributeDisplayNameFilter, //
            @ApiParam(value = "Get attributes with specified category")
            @RequestParam(value = "category") String category, //
            @ApiParam(value = "Get attributes with specified subcategory")
            @RequestParam(value = "subcategory") String subcategory, //
            @ApiParam(value = "Should get only selected attribute")
            @RequestParam(value = "onlySelectedAttributes", required = false) Boolean onlySelectedAttributes, //
            @ApiParam(value = "Offset for pagination of matching attributes")
            @RequestParam(value = "offset", required = false) Integer offset, //
            @ApiParam(value = "Maximum number of matching attributes in page")
            @RequestParam(value = "max", required = false) Integer max, //
            @ApiParam(value = "Consider internal attributes") //
            @RequestParam(value = "considerInternalAttributes", required = false, defaultValue = "false") Boolean considerInternalAttributes) {
        Tenant tenant = manufactureSecurityContextForInternalAccess(tenantId);
        Category categoryEnum = (StringStandardizationUtils.objectIsNullOrEmptyString(category) ? null : Category.fromName(category));
        return attributeService.getAttributes(tenant, attributeDisplayNameFilter, categoryEnum, subcategory,
                onlySelectedAttributes, offset, max, considerInternalAttributes);
    }

    private boolean containsAtLeastOneAttributeForCategory(List<LeadEnrichmentAttribute> allAttributes, Category category) {
        if (!CollectionUtils.isEmpty(allAttributes)) {
            for (LeadEnrichmentAttribute attr : allAttributes) {
                if (category.toString().equals(attr.getCategory())) {
                    return true;
                }
            }
        }
        return false;
    }

    private Tenant manufactureSecurityContextForInternalAccess(String tenantId) {
        log.info("Manufacturing security context for " + tenantId);
        Tenant tenant = tenantService.findByTenantId(tenantId);
        if (tenant == null) {
            throw new LedpException(LedpCode.LEDP_18074, new String[]{tenantId});
        }
        manufactureSecurityContextForInternalAccess(tenant);
        return tenant;
    }

    private void manufactureSecurityContextForInternalAccess(Tenant tenant) {
        TicketAuthenticationToken auth = new TicketAuthenticationToken(null, "x.y");
        Session session = new Session();
        session.setTenant(tenant);
        auth.setSession(session);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

}
