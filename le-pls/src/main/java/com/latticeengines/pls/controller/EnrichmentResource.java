package com.latticeengines.pls.controller;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.common.exposed.util.StringUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFactQuery;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatistics;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatsDetails;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributes;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributes.TopAttribute;
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

    public static final String AM_STATS_PATH = "/stats";

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
        List<LeadEnrichmentAttribute> allAttributes = getLeadEnrichmentAttributes(request, null, null, null, false,
                null, null);

        List<String> categoryStrList = new ArrayList<>();
        for (Category category : Category.values()) {
            if (containsAtleastOneAttributeForCategory(allAttributes, category)) {
                categoryStrList.add(category.toString());
            }
        }
        return categoryStrList;
    }

    @RequestMapping(value = LEAD_ENRICH_PATH + "/subcategories", method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of subcategories for a given category")
    public List<String> getLeadEnrichmentSubcategories(HttpServletRequest request, //
            @ApiParam(value = "category", required = true) //
            @RequestParam String category) {
        Set<String> subcategories = new HashSet<String>();
        List<LeadEnrichmentAttribute> allAttributes = getLeadEnrichmentAttributes(request, null, category, null, false,
                null, null);

        for (LeadEnrichmentAttribute attr : allAttributes) {
            subcategories.add(attr.getSubcategory());
        }
        return new ArrayList<String>(subcategories);
    }

    @RequestMapping(value = LEAD_ENRICH_PATH, //
            method = RequestMethod.PUT, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Save lead enrichment selection")
    public void saveLeadEnrichmentAttributes(HttpServletRequest request, //
            @ApiParam(value = "Update lead enrichment selection", required = true) //
            @RequestBody LeadEnrichmentAttributesOperationMap attributes) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        Boolean considerInternalAttributes = shouldConsiderInternalAttributes(tenant);
        selectedAttrService.save(attributes, tenant, getLeadEnrichmentPremiumAttributesLimitation(request),
                considerInternalAttributes);
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
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        Boolean considerInternalAttributes = shouldConsiderInternalAttributes(tenant);
        Category categoryEnum = (StringUtils.objectIsNullOrEmptyString(category) ? null : Category.fromName(category));
        return selectedAttrService.getAttributes(tenant, attributeDisplayNameFilter, categoryEnum, subcategory,
                onlySelectedAttributes, offset, max, considerInternalAttributes);
    }

    @RequestMapping(value = LEAD_ENRICH_PATH + "/count", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of attributes with selection flag")
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
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        Boolean considerInternalAttributes = shouldConsiderInternalAttributes(tenant);
        Category categoryEnum = (StringUtils.objectIsNullOrEmptyString(category) ? null : Category.fromName(category));
        return selectedAttrService.getAttributesCount(tenant, attributeDisplayNameFilter, categoryEnum, subcategory,
                onlySelectedAttributes, considerInternalAttributes);
    }

    @RequestMapping(value = LEAD_ENRICH_PATH
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
        selectedAttrService.downloadAttributes(request, response, "application/csv", fileName, tenant,
                onlySelectedAttributes, considerInternalAttributes);
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
        Boolean considerInternalAttributes = shouldConsiderInternalAttributes(tenant);
        return selectedAttrService.getSelectedAttributeCount(tenant, considerInternalAttributes);
    }

    @RequestMapping(value = LEAD_ENRICH_PATH + "/selectedpremiumattributes/count", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get selected premium attributes count")
    public Integer getLeadEnrichmentSelectedAttributePremiumCount(HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        Boolean considerInternalAttributes = shouldConsiderInternalAttributes(tenant);
        return selectedAttrService.getSelectedAttributePremiumCount(tenant, considerInternalAttributes);
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

    // ------------END for LeadEnrichment-------------------//

    // ------------START for statistics---------------------//
    @RequestMapping(value = AM_STATS_PATH, //
            method = RequestMethod.POST, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Load account master cube based on dimension selection")
    public AccountMasterCube loadAMStatisticsCube(HttpServletRequest request, //
            @RequestBody AccountMasterFactQuery query) {
        return createDummyCube();
    }

    @RequestMapping(value = AM_STATS_PATH, //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get top N attributes per subcategory for a given category")
    public TopNAttributes getTopNAttributes(HttpServletRequest request, //
            @ApiParam(value = "category", required = true) //
            @RequestParam String category, //
            @ApiParam(value = "category", required = false, defaultValue = "5") //
            @RequestParam Integer max) {
        return createTopNAttributes(category, max);
    }

    private AccountMasterCube createDummyCube() {
        List<LeadEnrichmentAttribute> allAttrs = selectedAttrService.getAllAttributes();
        return createDummyCube(allAttrs);
    }

    private AccountMasterCube createDummyCube(List<LeadEnrichmentAttribute> allAttrs) {
        AccountMasterCube cube = new AccountMasterCube();

        Map<String, AttributeStatistics> statistics = new HashMap<>();

        int count = 500000;
        for (LeadEnrichmentAttribute attr : allAttrs) {
            AttributeStatistics value = new AttributeStatistics();
            AttributeStatsDetails rowBasedStatistics = new AttributeStatsDetails();
            rowBasedStatistics.setNonNullCount(count--);
            Buckets buckets = new Buckets();
            buckets.setType(BucketType.Numerical);
            List<Bucket> bucketList = new ArrayList<>();
            Bucket bucket = new Bucket();
            bucket.setBucketLabel("First Bucket");
            bucket.setCount(count - 10);
            bucketList.add(bucket);
            bucket = new Bucket();
            bucket.setBucketLabel("Second Bucket");
            bucket.setCount(10);
            bucketList.add(bucket);
            buckets.setBucketList(bucketList);
            rowBasedStatistics.setBuckets(buckets);
            value.setRowBasedStatistics(rowBasedStatistics);

            statistics.put(attr.getFieldName(), value);
        }

        cube.setStatistics(statistics);
        return cube;
    }

    private TopNAttributes createTopNAttributes(String category, int max) {
        List<LeadEnrichmentAttribute> allAttrs = selectedAttrService.getAllAttributes();
        TopNAttributes topNAttributes = new TopNAttributes();
        AccountMasterCube cube = createDummyCube(allAttrs);
        Map<String, List<TopAttribute>> topAttributes = new HashMap<>();
        topNAttributes.setTopAttributes(topAttributes);

        for (LeadEnrichmentAttribute attr : allAttrs) {
            if (!attr.getCategory().equalsIgnoreCase(category)) {
                continue;
            }

            String subcategory = attr.getSubcategory();
            if (!topAttributes.containsKey(subcategory)) {
                List<TopAttribute> topNList = new ArrayList<>();
                topAttributes.put(subcategory, topNList);
            }
            List<TopAttribute> topNList = topAttributes.get(subcategory);

            updateTopNList(attr, max, topNList, cube.getStatistics().get(attr.getFieldName()).getRowBasedStatistics());

        }

        return topNAttributes;
    }

    private void updateTopNList(LeadEnrichmentAttribute attr, int max, List<TopAttribute> topNList,
            AttributeStatsDetails rowBasedStatistics) {
        if (topNList.size() >= max) {
            return;
        } else {
            TopAttribute topAttr = new TopAttribute();
            topAttr.setAttribute(attr.getFieldName());
            topAttr.setNonNullCount(rowBasedStatistics.getNonNullCount());
            topNList.add(topAttr);
        }
    }

    // ------------End for statistics---------------------//
}
