package com.latticeengines.scoringapi.controller;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
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
@RequestMapping(value = "/enrichment")
public class EnrichmentResource {

    @Autowired
    private OAuthUserEntityMgr oAuthUserEntityMgr;

    @Autowired
    private BatonService batonService;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    @Autowired
    private PlsInternalProxy plsInternalProxy;

    // ------------START for LeadEnrichment-------------------//
    @RequestMapping(value = "/categories", method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of categories")
    public List<String> getLeadEnrichmentCategories(HttpServletRequest request) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        return plsInternalProxy.getLeadEnrichmentCategories(customerSpace);
    }

    @RequestMapping(value = "/subcategories", method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of subcategories for a given category")
    public List<String> getLeadEnrichmentSubcategories(HttpServletRequest request, //
            @ApiParam(value = "category", required = true) //
            @RequestParam String category) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        return plsInternalProxy.getLeadEnrichmentSubcategories(customerSpace, category);
    }

    // NOTE - anoop - based on discussion with Jeff, we'enable put operation if
    // PM ask for it
    // @RequestMapping(value = "", //
    // method = RequestMethod.PUT, //
    // headers = "Accept=application/json")
    // @ResponseBody
    // @ApiOperation(value = "Save lead enrichment selection")
    // public void saveLeadEnrichmentAttributes(HttpServletRequest request, //
    // @ApiParam(value = "Update lead enrichment selection", required = true) //
    // @RequestBody LeadEnrichmentAttributesOperationMap attributes) {
    // try {
    // CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request,
    // oAuthUserEntityMgr);
    // internalResourceRestApiProxy.saveLeadEnrichmentAttributes(customerSpace,
    // attributes);
    // } catch (Exception e) {
    // throw new LedpException(LedpCode.LEDP_31112, new String[] {
    // e.getMessage() });
    // }
    // }

    @RequestMapping(method = RequestMethod.GET, //
            headers = "Accept=application/json")
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

    @RequestMapping(value = "/count", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
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

    @RequestMapping(value = "/premiumattributeslimitation", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get premium attributes limitation")
    public Map<String, Integer> getLeadEnrichmentPremiumAttributesLimitation(HttpServletRequest request) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        return plsInternalProxy.getPremiumAttributesLimitation(customerSpace);
    }

    @RequestMapping(value = "/selectedattributes/count", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get selected attributes count")
    public Integer getLeadEnrichmentSelectedAttributeCount(HttpServletRequest request) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        return plsInternalProxy.getSelectedAttributeCount(customerSpace,
                ScoreUtils.canEnrichInternalAttributes(batonService, customerSpace));
    }

    @RequestMapping(value = "/selectedpremiumattributes/count", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get selected premium attributes count")
    public Integer getLeadEnrichmentSelectedAttributePremiumCount(HttpServletRequest request) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        return plsInternalProxy.getSelectedAttributePremiumCount(customerSpace,
                ScoreUtils.canEnrichInternalAttributes(batonService, customerSpace));
    }

    // ------------END for LeadEnrichment-------------------//
}
