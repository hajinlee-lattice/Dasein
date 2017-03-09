package com.latticeengines.pls.controller;

import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.app.exposed.service.AttributeService;
import com.latticeengines.app.exposed.service.EnrichmentService;
import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFactQuery;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributes;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributes.TopAttribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.util.SecurityUtils;
import com.wordnik.swagger.annotations.ApiParam;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "latticeinsights", description = "REST resource for Lattice Insights")
@RestController
@RequestMapping(value = "/latticeinsights")
@PreAuthorize("hasRole('Edit_PLS_Configurations')")
public class LatticeInsightsResource {

    public static final String INSIGHTS_PATH = "/insights";

    public static final String AM_STATS_PATH = "/stats";

    private static final ObjectMapper OM = new ObjectMapper();

    @Autowired
    private SessionService sessionService;

    @Autowired
    private AttributeService attributeService;

    @Autowired
    private EnrichmentService enrichmentService;

    // ------------START for Insights-------------------//
    @RequestMapping(value = INSIGHTS_PATH + "/categories", method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of categories")
    public List<String> getInsightsCategories(HttpServletRequest request) {
        List<LeadEnrichmentAttribute> allAttributes = getInsightsAttributes(request, null, null, null, false, null,
                null);

        List<String> categoryStrList = new ArrayList<>();
        for (Category category : Category.values()) {
            if (containsAtleastOneAttributeForCategory(allAttributes, category)) {
                categoryStrList.add(category.toString());
            }
        }
        return categoryStrList;
    }

    @RequestMapping(value = INSIGHTS_PATH + "/subcategories", method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of subcategories for a given category")
    public List<String> getInsightsSubcategories(HttpServletRequest request, //
            @ApiParam(value = "category", required = true) //
            @RequestParam String category) {
        Set<String> subcategories = new HashSet<String>();
        List<LeadEnrichmentAttribute> allAttributes = getInsightsAttributes(request, null, category, null, false, null,
                null);

        for (LeadEnrichmentAttribute attr : allAttributes) {
            subcategories.add(attr.getSubcategory());
        }
        return new ArrayList<String>(subcategories);
    }

    @RequestMapping(value = INSIGHTS_PATH, //
            method = RequestMethod.PUT, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Save lead enrichment selection")
    public void saveInsightsAttributes(HttpServletRequest request, //
            @ApiParam(value = "Update lead enrichment selection", required = true) //
            @RequestBody LeadEnrichmentAttributesOperationMap attributes) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        Boolean considerInternalAttributes = shouldConsiderInternalAttributes(tenant);
        attributeService.save(attributes, tenant, getInsightsPremiumAttributesLimitation(request),
                considerInternalAttributes);
    }

    @RequestMapping(value = INSIGHTS_PATH, //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of attributes with selection flag", response = List.class)
    public void getInsightsAttributes(HttpServletRequest request, //
            HttpServletResponse response, //
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
            Integer max) {
        List<LeadEnrichmentAttribute> result = getInsightsAttributes(request, attributeDisplayNameFilter, category,
                subcategory, onlySelectedAttributes, offset, max);
        writeToGzipStream(response, result);
    }

    private List<LeadEnrichmentAttribute> getInsightsAttributes(HttpServletRequest request,
            String attributeDisplayNameFilter, //
            String category, //
            String subcategory, //
            Boolean onlySelectedAttributes, //
            Integer offset, //
            Integer max) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        Boolean considerInternalAttributes = shouldConsiderInternalAttributes(tenant);
        Category categoryEnum = (StringStandardizationUtils.objectIsNullOrEmptyString(category) ? null
                : Category.fromName(category));
        List<LeadEnrichmentAttribute> attributes = attributeService.getAttributes(tenant, attributeDisplayNameFilter,
                categoryEnum, subcategory, onlySelectedAttributes, offset, max, considerInternalAttributes);
        return attributes;
    }

    @RequestMapping(value = INSIGHTS_PATH + "/count", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of attributes with selection flag")
    public int getInsightsAttributesCount(HttpServletRequest request,
            @ApiParam(value = "Get attributes with name containing specified " //
                    + "text for attributeDisplayNameFilter") //
            @RequestParam(value = "attributeDisplayNameFilter", required = false) //
            String attributeDisplayNameFilter, //
            @ApiParam(value = "Get attributes with specified category") //
            @RequestParam(value = "category", required = false) //
            String category, //
            @ApiParam(value = "Get attributes with specified subcategory") //
            @RequestParam(value = "subcategory", required = false) //
            String subcategory, //
            @ApiParam(value = "Should get only selected attribute") //
            @RequestParam(value = "onlySelectedAttributes", required = false) //
            Boolean onlySelectedAttributes) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        Boolean considerInternalAttributes = shouldConsiderInternalAttributes(tenant);
        Category categoryEnum = (StringStandardizationUtils.objectIsNullOrEmptyString(category) ? null
                : Category.fromName(category));
        return attributeService.getAttributesCount(tenant, attributeDisplayNameFilter, categoryEnum, subcategory,
                onlySelectedAttributes, considerInternalAttributes);
    }

    @RequestMapping(value = INSIGHTS_PATH
            + "/downloadcsv", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Download lead enrichment attributes")
    public void downloadEnrichmentCSV(HttpServletRequest request, HttpServletResponse response,
            @ApiParam(value = "Should get only selected attribute", //
                    required = false) //
            @RequestParam(value = "onlySelectedAttributes", required = false) //
            Boolean onlySelectedAttributes) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        Boolean considerInternalAttributes = shouldConsiderInternalAttributes(tenant);
        DateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");
        String dateString = dateFormat.format(new Date());
        String fileName = onlySelectedAttributes != null && onlySelectedAttributes
                ? String.format("selectedEnrichmentAttributes_%s.csv", dateString)
                : String.format("enrichmentAttributes_%s.csv", dateString);
        attributeService.downloadAttributes(request, response, "application/csv", fileName, tenant,
                onlySelectedAttributes, considerInternalAttributes);
    }

    @RequestMapping(value = INSIGHTS_PATH + "/premiumattributeslimitation", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get premium attributes limitation")
    public Map<String, Integer> getInsightsPremiumAttributesLimitation(HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        return attributeService.getPremiumAttributesLimitation(tenant);
    }

    @RequestMapping(value = INSIGHTS_PATH + "/selectedattributes/count", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get selected attributes count")
    public Integer getInsightsSelectedAttributeCount(HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        Boolean considerInternalAttributes = shouldConsiderInternalAttributes(tenant);
        return attributeService.getSelectedAttributeCount(tenant, considerInternalAttributes);
    }

    @RequestMapping(value = INSIGHTS_PATH + "/selectedpremiumattributes/count", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get selected premium attributes count")
    public Integer getInsightsSelectedAttributePremiumCount(HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        Boolean considerInternalAttributes = shouldConsiderInternalAttributes(tenant);
        return attributeService.getSelectedAttributePremiumCount(tenant, considerInternalAttributes);
    }

    private boolean containsAtleastOneAttributeForCategory(List<LeadEnrichmentAttribute> allAttributes,
            Category category) {
        if (!CollectionUtils.isEmpty(allAttributes)) {
            for (LeadEnrichmentAttribute attr : allAttributes) {
                if (category.toString().equals(attr.getCategory())) {
                    return true;
                }
            }
        }
        return false;
    }

    private Boolean shouldConsiderInternalAttributes(Tenant tenant) {
        CustomerSpace space = CustomerSpace.parse(tenant.getId());
        Boolean considerInternalAttributes = FeatureFlagClient.isEnabled(space,
                LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES.getName());
        return considerInternalAttributes;
    }

    // ------------END for Insights-------------------//

    // ------------START for statistics---------------------//
    @RequestMapping(value = AM_STATS_PATH + "/cube", //
            method = RequestMethod.POST, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Load account master cube based on dimension selection", response = AccountMasterCube.class)
    public void loadAMStatisticsCubeByPost(HttpServletRequest request, //
            HttpServletResponse response, //
            @ApiParam(value = "Should load enrichment attribute metadata") //
            @RequestParam(value = "loadEnrichmentMetadata", required = false, defaultValue = "false") //
            Boolean loadEnrichmentMetadata, //
            @RequestBody(required = false) AccountMasterFactQuery query) {
        AccountMasterCube cube = enrichmentService.getCube(query);

        if (loadEnrichmentMetadata) {
            List<LeadEnrichmentAttribute> enrichmentAttributes = getInsightsAttributes(request, null, null, null, null,
                    null, null);
            cube.setEnrichmentAttributes(enrichmentAttributes);
        }
        writeToGzipStream(response, cube);
    }

    @RequestMapping(value = AM_STATS_PATH + "/cube", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Load account master cube based on dimension selection", response = AccountMasterCube.class)
    public void loadAMStatisticsCube(HttpServletRequest request, //
            HttpServletResponse response, //
            @ApiParam(value = "Should load enrichment attribute metadata") //
            @RequestParam(value = "loadEnrichmentMetadata", required = false, defaultValue = "false") //
            Boolean loadEnrichmentMetadata, //
            @RequestParam(value = "q", required = false) String query) {
        AccountMasterCube cube = enrichmentService.getCube(query);

        if (loadEnrichmentMetadata) {
            List<LeadEnrichmentAttribute> enrichmentAttributes = getInsightsAttributes(request, null, null, null, null,
                    null, null);
            cube.setEnrichmentAttributes(enrichmentAttributes);
        }
        writeToGzipStream(response, cube);
    }

    @RequestMapping(value = AM_STATS_PATH + "/topn", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get top N attributes per subcategory for a given category")
    public TopNAttributes getTopNAttributes(HttpServletRequest request, //
            @ApiParam(value = "Should load enrichment attribute metadata") //
            @RequestParam(value = "loadEnrichmentMetadata", required = false, defaultValue = "false") //
            Boolean loadEnrichmentMetadata, //
            @ApiParam(value = "category", required = true) //
            @RequestParam(value = "category") String categoryName, //
            @ApiParam(value = "max", defaultValue = "5") //
            @RequestParam(value = "max", required = false, defaultValue = "5") Integer max) {
        Category category;
        try {
            category = Category.fromName(categoryName);
        } catch (Exception e) {
            try {
                category = Category.valueOf(categoryName);
            } catch (Exception e1) {
                throw new RuntimeException("Cannot recognize category name " + categoryName, e1);
            }
        }

        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        Boolean considerInternalAttributes = shouldConsiderInternalAttributes(tenant);

        TopNAttributes topNAttr = enrichmentService.getTopAttrs(category, max,
                considerInternalAttributes == null ? true : considerInternalAttributes);

        if (loadEnrichmentMetadata) {
            Set<String> allEnrichAttrNames = new HashSet<>();
            for (String subCategoryKey : topNAttr.getTopAttributes().keySet()) {
                for (TopAttribute attr : topNAttr.getTopAttributes().get(subCategoryKey)) {
                    allEnrichAttrNames.add(attr.getAttribute());
                }
            }
            List<LeadEnrichmentAttribute> enrichmentAttributes = getInsightsAttributes(request, null, categoryName,
                    null, null, null, null);
            List<LeadEnrichmentAttribute> attrs = new ArrayList<>();

            for (LeadEnrichmentAttribute attr : enrichmentAttributes) {
                if (allEnrichAttrNames.contains(attr.getFieldName())) {
                    attrs.add(attr);
                }
            }

            topNAttr.setEnrichmentAttributes(attrs);
        }

        return topNAttr;
    }

    @RequestMapping(value = AM_STATS_PATH + "/topn/all", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get top N attributes per subcategory for each category")
    public Map<String, TopNAttributes> getAllTopNAttributes(HttpServletRequest request, //
            @ApiParam(value = "Should load enrichment attribute metadata") //
            @RequestParam(value = "loadEnrichmentMetadata", required = false, defaultValue = "false") //
            Boolean loadEnrichmentMetadata, //
            @ApiParam(value = "max", defaultValue = "5") //
            @RequestParam(value = "max", required = false, defaultValue = "5") Integer max) {
        List<String> categories = getInsightsCategories(request);
        Map<String, TopNAttributes> allTopNAttributes = new HashMap<>();

        for (String categoryName : categories) {
            TopNAttributes topNAttrs = getTopNAttributes(request, loadEnrichmentMetadata, categoryName, max);
            allTopNAttributes.put(categoryName, topNAttrs);
        }

        return allTopNAttributes;
    }

    // ------------End for statistics---------------------//

    private void writeToGzipStream(HttpServletResponse response, Object output) {
        try {
            OutputStream os = response.getOutputStream();
            response.setHeader("Content-Encoding", "gzip");
            response.setHeader("Content-Type", "application/json");
            GzipCompressorOutputStream gzipOs = new GzipCompressorOutputStream(os);
            OM.writeValue(gzipOs, output);
            gzipOs.flush();
            gzipOs.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
