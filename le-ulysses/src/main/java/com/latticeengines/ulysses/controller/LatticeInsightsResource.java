package com.latticeengines.ulysses.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;

@Api(value = "latticeinsights", description = "REST resource for Lattice Insights")
@RestController
@RequestMapping(value = "/latticeinsights")
public class LatticeInsightsResource {
//    public static final String INSIGHTS_PATH = "/insights";
//
//    public static final String STATS_PATH = "/stats";
//
//    @Autowired
//    private AttributeService attributeService;
//
//    @Autowired
//    private EnrichmentService enrichmentService;
//
//    // ------------START for LeadEnrichment-------------------//
//    @RequestMapping(value = INSIGHTS_PATH + "/categories", method = RequestMethod.GET, //
//            headers = "Accept=application/json")
//    @ResponseBody
//    @ApiOperation(value = "Get list of categories")
//    public List<String> getInsightsCategories(HttpServletRequest request) {
//        List<LeadEnrichmentAttribute> allAttributes = getInsightsAttributes(request, null, null, null, false, null,
//                null);
//
//        List<String> categoryStrList = new ArrayList<>();
//        for (Category category : Category.values()) {
//            if (containsAtleastOneAttributeForCategory(allAttributes, category)) {
//                categoryStrList.add(category.toString());
//            }
//        }
//        return categoryStrList;
//    }
//
//    @RequestMapping(value = INSIGHTS_PATH + "/subcategories", method = RequestMethod.GET, //
//            headers = "Accept=application/json")
//    @ResponseBody
//    @ApiOperation(value = "Get list of subcategories for a given category")
//    public List<String> getInsightsSubcategories(HttpServletRequest request, //
//            @ApiParam(value = "category", required = true) //
//            @RequestParam String category) {
//        Set<String> subcategories = new HashSet<String>();
//        List<LeadEnrichmentAttribute> allAttributes = getInsightsAttributes(request, null, category, null, false, null,
//                null);
//
//        for (LeadEnrichmentAttribute attr : allAttributes) {
//            subcategories.add(attr.getSubcategory());
//        }
//        return new ArrayList<String>(subcategories);
//    }
//
//    @RequestMapping(value = INSIGHTS_PATH, //
//            method = RequestMethod.GET, //
//            headers = "Accept=application/json")
//    @ResponseBody
//    @ApiOperation(value = "Get list of attributes with selection flag")
//    public List<LeadEnrichmentAttribute> getInsightsAttributes(HttpServletRequest request, //
//            @ApiParam(value = "Get attributes with name containing specified " //
//                    + "text for attributeDisplayNameFilter", required = false) //
//            @RequestParam(value = "attributeDisplayNameFilter", required = false) //
//            String attributeDisplayNameFilter, //
//            @ApiParam(value = "Get attributes " //
//                    + "with specified category", required = false) //
//            @RequestParam(value = "category", required = false) //
//            String category, //
//            @ApiParam(value = "Get attributes " //
//                    + "with specified subcategory", required = false) //
//            @RequestParam(value = "subcategory", required = false) //
//            String subcategory, //
//            @ApiParam(value = "Should get only selected attribute", //
//                    required = false) //
//            @RequestParam(value = "onlySelectedAttributes", required = false) //
//            Boolean onlySelectedAttributes, //
//            @ApiParam(value = "Offset for pagination of matching attributes", required = false) //
//            @RequestParam(value = "offset", required = false) //
//            Integer offset, //
//            @ApiParam(value = "Maximum number of matching attributes in page", required = false) //
//            @RequestParam(value = "max", required = false) //
//            Integer max) {
//        Tenant tenant = MultiTenantContext.getTenant();
//        Boolean considerInternalAttributes = shouldConsiderInternalAttributes(tenant);
//        Category categoryEnum = (StringStandardizationUtils.objectIsNullOrEmptyString(category) ? null
//                : Category.fromName(category));
//        List<LeadEnrichmentAttribute> attributes = attributeService.getAttributes(tenant, attributeDisplayNameFilter,
//                categoryEnum, subcategory, onlySelectedAttributes, offset, max, considerInternalAttributes);
//        return attributes;
//    }
//
//    @RequestMapping(value = INSIGHTS_PATH + "/count", //
//            method = RequestMethod.GET, //
//            headers = "Accept=application/json")
//    @ResponseBody
//    @ApiOperation(value = "Get list of attributes with selection flag")
//    public int getInsightsAttributesCount(HttpServletRequest request,
//            @ApiParam(value = "Get attributes with name containing specified " //
//                    + "text for attributeDisplayNameFilter", required = false) //
//            @RequestParam(value = "attributeDisplayNameFilter", required = false) //
//            String attributeDisplayNameFilter, //
//            @ApiParam(value = "Get attributes " //
//                    + "with specified category", required = false) //
//            @RequestParam(value = "category", required = false) //
//            String category, //
//            @ApiParam(value = "Get attributes " //
//                    + "with specified subcategory", required = false) //
//            @RequestParam(value = "subcategory", required = false) //
//            String subcategory, //
//            @ApiParam(value = "Should get only selected attribute", //
//                    required = false) //
//            @RequestParam(value = "onlySelectedAttributes", required = false) //
//            Boolean onlySelectedAttributes) {
//        Tenant tenant = MultiTenantContext.getTenant();
//        Boolean considerInternalAttributes = shouldConsiderInternalAttributes(tenant);
//        Category categoryEnum = (StringStandardizationUtils.objectIsNullOrEmptyString(category) ? null
//                : Category.fromName(category));
//        return attributeService.getCounts(tenant, attributeDisplayNameFilter, categoryEnum, subcategory,
//                onlySelectedAttributes, considerInternalAttributes);
//    }
//
//    @RequestMapping(value = INSIGHTS_PATH
//            + "/downloadcsv", method = RequestMethod.GET, headers = "Accept=application/json")
//    @ResponseBody
//    @ApiOperation(value = "Download lead enrichment attributes")
//    public void downloadInsightsCSV(HttpServletRequest request, HttpServletResponse response,
//            @ApiParam(value = "Should get only selected attribute", //
//                    required = false) //
//            @RequestParam(value = "onlySelectedAttributes", required = false) //
//            Boolean onlySelectedAttributes) {
//        Tenant tenant = MultiTenantContext.getTenant();
//        Boolean considerInternalAttributes = shouldConsiderInternalAttributes(tenant);
//        DateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");
//        String dateString = dateFormat.format(new Date());
//        String fileName = onlySelectedAttributes != null && onlySelectedAttributes
//                ? String.format("selectedEnrichmentAttributes_%s.csv", dateString)
//                : String.format("enrichmentAttributes_%s.csv", dateString);
//        attributeService.downloadAttributes(request, response, "application/csv", fileName, tenant,
//                onlySelectedAttributes, considerInternalAttributes);
//    }
//
//    @RequestMapping(value = INSIGHTS_PATH + "/premiumattributeslimitation", //
//            method = RequestMethod.GET, //
//            headers = "Accept=application/json")
//    @ResponseBody
//    @ApiOperation(value = "Get premium attributes limitation")
//    public Map<String, Integer> getInsightssPremiumAttributesLimitation(HttpServletRequest request) {
//        Tenant tenant = MultiTenantContext.getTenant();
//        return attributeService.getPremiumAttributesLimitation(tenant);
//    }
//
//    private boolean containsAtleastOneAttributeForCategory(List<LeadEnrichmentAttribute> allAttributes,
//            Category category) {
//        if (!CollectionUtils.isEmpty(allAttributes)) {
//            for (LeadEnrichmentAttribute attr : allAttributes) {
//                if (category.toString().equals(attr.getCategory())) {
//                    return true;
//                }
//            }
//        }
//        return false;
//    }
//
//    private Boolean shouldConsiderInternalAttributes(Tenant tenant) {
//        CustomerSpace space = CustomerSpace.parse(tenant.getId());
//        Boolean considerInternalAttributes = FeatureFlagClient.isEnabled(space,
//                LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES.getName());
//        return considerInternalAttributes;
//    }
//
//    // ------------END for LeadEnrichment-------------------//
//
//    // ------------START for statistics---------------------//
//    @RequestMapping(value = STATS_PATH + "/cube", //
//            method = RequestMethod.POST, //
//            headers = "Accept=application/json")
//    @ResponseBody
//    @ApiOperation(value = "Load account master cube based on dimension selection", response = AccountMasterCube.class)
//    public AccountMasterCube loadAMStatisticsCubeByPost(HttpServletRequest request, //
//            @ApiParam(value = "Should load enrichment attribute metadata") //
//            @RequestParam(value = "loadEnrichmentMetadata", required = false, defaultValue = "false") //
//            Boolean loadEnrichmentMetadata, //
//            @RequestBody(required = false) AccountMasterFactQuery query) {
//        AccountMasterCube cube = enrichmentService.getCube(query);
//
//        if (loadEnrichmentMetadata) {
//            List<LeadEnrichmentAttribute> enrichmentAttributes = getInsightsAttributes(request, null, null, null, null,
//                    null, null);
//            cube.setEnrichmentAttributes(enrichmentAttributes);
//        }
//        return cube;
//    }
//
//    @RequestMapping(value = STATS_PATH + "/cube", //
//            method = RequestMethod.GET, //
//            headers = "Accept=application/json")
//    @ResponseBody
//    @ApiOperation(value = "Load account master cube based on dimension selection", response = AccountMasterCube.class)
//    public AccountMasterCube loadAMStatisticsCube(HttpServletRequest request, //
//            @ApiParam(value = "Should load enrichment attribute metadata") //
//            @RequestParam(value = "loadEnrichmentMetadata", required = false, defaultValue = "false") //
//            Boolean loadEnrichmentMetadata, //
//            @RequestParam(value = "q", required = false) String query) {
//        AccountMasterCube cube = enrichmentService.getCube(query);
//        if (loadEnrichmentMetadata) {
//            List<LeadEnrichmentAttribute> enrichmentAttributes = getInsightsAttributes(request, null, null, null, null,
//                    null, null);
//            cube.setEnrichmentAttributes(enrichmentAttributes);
//        }
//        return cube;
//    }
//
//    @RequestMapping(value = STATS_PATH + "/topn", //
//            method = RequestMethod.GET, //
//            headers = "Accept=application/json")
//    @ResponseBody
//    @ApiOperation(value = "Get top N attributes per subcategory for a given category")
//    public TopNAttributes getTopNAttributes(HttpServletRequest request, //
//            @ApiParam(value = "Should load enrichment attribute metadata") //
//            @RequestParam(value = "loadEnrichmentMetadata", required = false, defaultValue = "false") //
//            Boolean loadEnrichmentMetadata, //
//            @ApiParam(value = "category", required = true) //
//            @RequestParam(value = "category") String categoryName, //
//            @ApiParam(value = "max", defaultValue = "5") //
//            @RequestParam(value = "max", required = false, defaultValue = "5") Integer max) {
//        Category category;
//        try {
//            category = Category.fromName(categoryName);
//        } catch (Exception e) {
//            try {
//                category = Category.valueOf(categoryName);
//            } catch (Exception e1) {
//                throw new RuntimeException("Cannot recognize category name " + categoryName, e1);
//            }
//        }
//
//        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
//        Boolean considerInternalAttributes = FeatureFlagClient.isEnabled(customerSpace,
//                LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES.getName());
//
//        TopNAttributes topNAttr = enrichmentService.getTopAttrs(category, max,
//                considerInternalAttributes == null ? true : considerInternalAttributes);
//
//        if (loadEnrichmentMetadata) {
//            Set<String> allEnrichAttrNames = new HashSet<>();
//            for (String subCategoryKey : topNAttr.getTopAttributes().keySet()) {
//                for (TopAttribute attr : topNAttr.getTopAttributes().get(subCategoryKey)) {
//                    allEnrichAttrNames.add(attr.getAttribute());
//                }
//            }
//            List<LeadEnrichmentAttribute> enrichmentAttributes = getInsightsAttributes(request, null, categoryName,
//                    null, null, null, null);
//            List<LeadEnrichmentAttribute> attrs = new ArrayList<>();
//
//            for (LeadEnrichmentAttribute attr : enrichmentAttributes) {
//                if (allEnrichAttrNames.contains(attr.getFieldName())) {
//                    attrs.add(attr);
//                }
//            }
//
//            topNAttr.setEnrichmentAttributes(attrs);
//        }
//
//        return topNAttr;
//    }
//
//    @RequestMapping(value = STATS_PATH + "/topn/all", //
//            method = RequestMethod.GET, //
//            headers = "Accept=application/json")
//    @ResponseBody
//    @ApiOperation(value = "Get top N attributes per subcategory for each category")
//    public Map<String, TopNAttributes> getAllTopNAttributes(HttpServletRequest request, //
//            @ApiParam(value = "Should load enrichment attribute metadata") //
//            @RequestParam(value = "loadEnrichmentMetadata", required = false, defaultValue = "false") //
//            Boolean loadEnrichmentMetadata, //
//            @ApiParam(value = "max", defaultValue = "5") //
//            @RequestParam(value = "max", required = false, defaultValue = "5") Integer max) {
//        List<String> categories = getInsightsCategories(request);
//        Map<String, TopNAttributes> allTopNAttributes = new HashMap<>();
//
//        for (String categoryName : categories) {
//            TopNAttributes topNAttrs = getTopNAttributes(request, loadEnrichmentMetadata, categoryName, max);
//            allTopNAttributes.put(categoryName, topNAttrs);
//        }
//
//        return allTopNAttributes;
//    }
}
