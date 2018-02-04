package com.latticeengines.app.exposed.controller;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.download.DlFileHttpDownloader;
import com.latticeengines.app.exposed.service.AttributeService;
import com.latticeengines.app.exposed.service.EnrichmentService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.db.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "latticeinsights", description = "REST resource for Lattice Insights")
@RestController
@RequestMapping(value = "/latticeinsights")
public class LatticeInsightsResource {

    public static final String INSIGHTS_PATH = "/insights";

    public static final String SEGMENTS_PATH = "/segments";

    public static final String AM_STATS_PATH = "/stats";

    @Autowired
    private AttributeService attributeService;

    @Autowired
    private EnrichmentService enrichmentService;

    @Autowired
    private BatonService batonService;

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
        Tenant tenant = MultiTenantContext.getTenant();
        Boolean considerInternalAttributes = shouldConsiderInternalAttributes(tenant);
        attributeService.save(attributes, tenant, getInsightsPremiumAttributesLimitation(request),
                considerInternalAttributes);
    }

    @RequestMapping(value = INSIGHTS_PATH + "/save", //
            method = RequestMethod.PUT, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Save lead enrichment selection")
    public void saveInsightsSelectedAttributes(HttpServletRequest request, //
            @ApiParam(value = "Update lead enrichment selection", required = true) //
            @RequestBody LeadEnrichmentAttributesOperationMap attributes) {
        Tenant tenant = MultiTenantContext.getTenant();
        Boolean considerInternalAttributes = shouldConsiderInternalAttributes(tenant);
        attributeService.saveSelectedAttribute(attributes, tenant, getInsightsPremiumAttributesLimitationMap(request),
                considerInternalAttributes);
    }

    @RequestMapping(value = INSIGHTS_PATH, //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of attributes with selection flag", response = List.class)
    public List<LeadEnrichmentAttribute> getInsightsAttributes(HttpServletRequest request, //
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
            Boolean onlySelectedAttributes, //
            @ApiParam(value = "Offset for pagination of matching attributes") //
            @RequestParam(value = "offset", required = false) //
            Integer offset, //
            @ApiParam(value = "Maximum number of matching attributes in page") //
            @RequestParam(value = "max", required = false) //
            Integer max) {
        Tenant tenant = MultiTenantContext.getTenant();
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
        Tenant tenant = MultiTenantContext.getTenant();
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
            @ApiParam(value = "Should get only selected attribute") //
            @RequestParam(value = "onlySelectedAttributes", required = false) //
            Boolean onlySelectedAttributes) {
        Tenant tenant = MultiTenantContext.getTenant();
        Boolean considerInternalAttributes = shouldConsiderInternalAttributes(tenant);
        DateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");
        String dateString = dateFormat.format(new Date());
        String fileName = onlySelectedAttributes != null && onlySelectedAttributes
                ? String.format("selectedEnrichmentAttributes_%s.csv", dateString)
                : String.format("enrichmentAttributes_%s.csv", dateString);
        attributeService.downloadAttributes(request, response, "application/csv", fileName, tenant,
                onlySelectedAttributes, considerInternalAttributes);
    }

    private static final String SEGMENT_CONTACTS_FILE_LOCAL_PATH = "com/latticeengines/pls/controller/internal/export-state-%s.csv";

    @RequestMapping(value = SEGMENTS_PATH
            + "/downloadcsv", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Download lead enrichment attributes")
    public void downloadSegmentCSV(HttpServletRequest request, HttpServletResponse response,
            @RequestParam(value = "state", required = true) String state) throws FileNotFoundException, IOException {
        InputStream stream = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(String.format(SEGMENT_CONTACTS_FILE_LOCAL_PATH, state));
        String inputStream = IOUtils.toString(new InputStreamReader(stream));
        DlFileHttpDownloader downloader = new DlFileHttpDownloader("application/csv", "segments-contacts.csv",
                inputStream);
        downloader.downloadFile(request, response);
    }

    @RequestMapping(value = INSIGHTS_PATH + "/premiumattributeslimitation", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get premium attributes limitation")
    public Map<String, Integer> getInsightsPremiumAttributesLimitation(HttpServletRequest request) {
        Tenant tenant = MultiTenantContext.getTenant();
        return attributeService.getPremiumAttributesLimitation(tenant);
    }

    @RequestMapping(value = INSIGHTS_PATH + "/premiumattributeslimitationmap", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get premium attributes limitation")
    public Map<String, Integer> getInsightsPremiumAttributesLimitationMap(HttpServletRequest request) {
        Tenant tenant = MultiTenantContext.getTenant();
        return attributeService.getPremiumAttributesLimitationMap(tenant);
    }

    @RequestMapping(value = INSIGHTS_PATH + "/selectedattributes/count", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get selected attributes count")
    public Integer getInsightsSelectedAttributeCount(HttpServletRequest request) {
        Tenant tenant = MultiTenantContext.getTenant();
        Boolean considerInternalAttributes = shouldConsiderInternalAttributes(tenant);
        return attributeService.getSelectedAttributeCount(tenant, considerInternalAttributes);
    }

    @RequestMapping(value = INSIGHTS_PATH + "/selectedpremiumattributes/count", //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get selected premium attributes count")
    public Integer getInsightsSelectedAttributePremiumCount(HttpServletRequest request) {
        Tenant tenant = MultiTenantContext.getTenant();
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

    private boolean shouldConsiderInternalAttributes(Tenant tenant) {
        CustomerSpace space = CustomerSpace.parse(tenant.getId());
        return batonService.isEnabled(space, LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES);
    }

    // ------------END for Insights-------------------//

    // ------------START for statistics---------------------//
    @RequestMapping(value = AM_STATS_PATH + "/cube", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Load account master cube", response = AccountMasterCube.class)
    public StatsCube loadAMStatisticsCube() {
        return enrichmentService.getStatsCube();
    }

    @RequestMapping(value = AM_STATS_PATH + "/cubes", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Load account master cube as a map", response = AccountMasterCube.class)
    public Map<BusinessEntity, StatsCube> getAMStatsCubes() {
        return enrichmentService.getStatsCubes();
    }

    @RequestMapping(value = AM_STATS_PATH + "/topn", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get top N attributes for all category")
    public TopNTree getTopTree() {
        Tenant tenant = MultiTenantContext.getTenant();
        boolean excludeInternalAttributes = !shouldConsiderInternalAttributes(tenant);
        return enrichmentService.getTopNTree(excludeInternalAttributes);
    }

}
