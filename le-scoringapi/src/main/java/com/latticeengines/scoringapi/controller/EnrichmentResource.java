package com.latticeengines.scoringapi.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.proxy.exposed.pls.PlsInternalProxy;
import com.latticeengines.scoringapi.exposed.ScoreUtils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "enrichment", description = "REST resource for enrichment configuration")
@RestController
@RequestMapping("/enrichment")
public class EnrichmentResource {

    @Inject
    private OAuthUserEntityMgr oAuthUserEntityMgr;

    @Inject
    private BatonService batonService;

    @Inject
    private PlsInternalProxy plsInternalProxy;

    // ------------START for LeadEnrichment-------------------//
    @GetMapping("/categories")
    @ResponseBody
    @ApiOperation(value = "Get list of categories")
    public List<String> getLeadEnrichmentCategories(HttpServletRequest request) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        return plsInternalProxy.getLeadEnrichmentCategories(customerSpace);
    }

    @GetMapping("/subcategories")
    @ResponseBody
    @ApiOperation(value = "Get list of subcategories for a given category")
    public List<String> getLeadEnrichmentSubcategories(HttpServletRequest request, //
            @ApiParam(value = "category", required = true) //
            @RequestParam String category) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        return plsInternalProxy.getLeadEnrichmentSubcategories(customerSpace, category);
    }

    @GetMapping()
    @ResponseBody
    @ApiOperation(value = "Get list of attributes with specified query parameters")
    public List<LeadEnrichmentAttribute> getLeadEnrichmentAttributes(HttpServletRequest request,
            @ApiParam(value = "Get attributes with name containing specified " //
                    + "text for attributeDisplayNameFilter", required = false) //
            @RequestParam(value = "attributeDisplayNameFilter", required = false) //
            String attributeDisplayNameFilter, //
            @ApiParam(value = "Get attributes " //
                    + "with specified category", required = false) //
            @RequestParam(value = "category", required = false) //
            String category, //
            @ApiParam(value = "Get attributes " //
                    + "with specified subcategory", required = false) //
            @RequestParam(value = "subcategory", required = false) //
            String subcategory, //
            @ApiParam(value = "Should get only selected attribute", //
                    required = false) //
            @RequestParam(value = "onlySelectedAttributes", required = false) //
            Boolean onlySelectedAttributes, //
            @ApiParam(value = "Offset for pagination of matching attributes", required = false) //
            @RequestParam(value = "offset", required = false) //
            Integer offset, //
            @ApiParam(value = "Maximum number of matching attributes in page", required = false) //
            @RequestParam(value = "max", required = false) //
            Integer max //
    ) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        Category categoryEnum = (StringStandardizationUtils.objectIsNullOrEmptyString(category) ? null
                : Category.fromName(category));
        return plsInternalProxy.getLeadEnrichmentAttributes(customerSpace, attributeDisplayNameFilter,
                categoryEnum, subcategory, onlySelectedAttributes, offset, max,
                ScoreUtils.canEnrichInternalAttributes(batonService, customerSpace));
    }

    @GetMapping("/count")
    @ResponseBody
    @ApiOperation(value = "Get count of attributes with specified query parameters")
    public int getLeadEnrichmentAttributesCount(HttpServletRequest request,
            @ApiParam(value = "Get attributes with name containing specified " //
                    + "text for attributeDisplayNameFilter", required = false) //
            @RequestParam(value = "attributeDisplayNameFilter", required = false) //
            String attributeDisplayNameFilter, //
            @ApiParam(value = "Get attributes " //
                    + "with specified category", required = false) //
            @RequestParam(value = "category", required = false) //
            String category, //
            @ApiParam(value = "Get attributes " //
                    + "with specified subcategory", required = false) //
            @RequestParam(value = "subcategory", required = false) //
            String subcategory, //
            @ApiParam(value = "Should get only selected attribute", //
                    required = false) //
            @RequestParam(value = "onlySelectedAttributes", required = false) //
            Boolean onlySelectedAttributes) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        Category categoryEnum = (StringStandardizationUtils.objectIsNullOrEmptyString(category) ? null
                : Category.fromName(category));
        return plsInternalProxy.getLeadEnrichmentAttributesCount(customerSpace, attributeDisplayNameFilter,
                categoryEnum, subcategory, onlySelectedAttributes,
                ScoreUtils.canEnrichInternalAttributes(batonService, customerSpace));
    }

    @GetMapping("/premiumattributeslimitation")
    @ResponseBody
    @ApiOperation(value = "Get premium attributes limitation")
    public Map<String, Integer> getLeadEnrichmentPremiumAttributesLimitation(HttpServletRequest request) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        return plsInternalProxy.getPremiumAttributesLimitation(customerSpace);
    }

    @GetMapping("/selectedattributes/count")
    @ResponseBody
    @ApiOperation(value = "Get selected attributes count")
    public Integer getLeadEnrichmentSelectedAttributeCount(HttpServletRequest request) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        return plsInternalProxy.getSelectedAttributeCount(customerSpace,
                ScoreUtils.canEnrichInternalAttributes(batonService, customerSpace));
    }

    @GetMapping("/selectedpremiumattributes/count")
    @ResponseBody
    @ApiOperation(value = "Get selected premium attributes count")
    public Integer getLeadEnrichmentSelectedAttributePremiumCount(HttpServletRequest request) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        return plsInternalProxy.getSelectedAttributePremiumCount(customerSpace,
                ScoreUtils.canEnrichInternalAttributes(batonService, customerSpace));
    }

    // ------------END for LeadEnrichment-------------------//
}
