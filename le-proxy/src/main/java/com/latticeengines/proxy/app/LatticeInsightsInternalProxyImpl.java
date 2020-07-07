package com.latticeengines.proxy.app;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;
import com.latticeengines.proxy.exposed.app.LatticeInsightsInternalProxy;

/*
 * this suppress warning is for DeprecatedBaseRestApiProxy class
 * the plan is to remove all the proxy that uses DeprecatedBaseRestApiProxy in the future,
 * so suppress for now.
 */
@Component("latticeInsightsInternalProxy")
public class LatticeInsightsInternalProxyImpl extends BaseRestApiProxy implements LatticeInsightsInternalProxy {

    private static final Logger log = LoggerFactory.getLogger(LatticeInsightsInternalProxyImpl.class);

    private static final String INSIGHTS_PATH = "/insights";

    private static final String LATTICE_INSIGHTS_INTERNAL_ENRICHMENT = "/internal/latticeinsights/enrichment";

    public LatticeInsightsInternalProxyImpl() {
        super(PropertyUtils.getProperty("common.pls.url"), "pls");
    }

    public LatticeInsightsInternalProxyImpl(String hostPort) {
        super(hostPort, "pls");
    }

    @Override
    public List<String> getLeadEnrichmentCategories(CustomerSpace customerSpace) {
        try {
            String url = constructUrl(combine(LATTICE_INSIGHTS_INTERNAL_ENRICHMENT + INSIGHTS_PATH + "/categories",
                    customerSpace.toString()));
            List<?> categoriesObjList = get("getLeadEnrichmentCategories", url, List.class);
            return JsonUtils.convertList(categoriesObjList, String.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[]{e.getMessage()});
        }
    }

    @Override
    public List<String> getLeadEnrichmentSubcategories(CustomerSpace customerSpace, String category) {
        try {
            String url = constructUrl(combine(LATTICE_INSIGHTS_INTERNAL_ENRICHMENT + INSIGHTS_PATH + "/subcategories",
                    customerSpace.toString()));
            url += "?category=" + category;
            List<?> subCategoriesObjList = get("getLeadEnrichmentSubcategories", url, List.class);
            return JsonUtils.convertList(subCategoriesObjList, String.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[]{e.getMessage()});
        }
    }

    @Override
    public List<LeadEnrichmentAttribute> getLeadEnrichmentAttributes(CustomerSpace customerSpace, //
                                                                     String attributeDisplayNameFilter, Category category, //
                                                                     Boolean onlySelectedAttributes) {
        return getLeadEnrichmentAttributes(customerSpace, attributeDisplayNameFilter, category, onlySelectedAttributes,
                Boolean.FALSE);
    }

    public List<LeadEnrichmentAttribute> getLeadEnrichmentAttributes(CustomerSpace customerSpace, //
                                                                     String attributeDisplayNameFilter, Category category, //
                                                                     Boolean onlySelectedAttributes, Boolean considerInternalAttributes) {
        return getLeadEnrichmentAttributes(customerSpace, attributeDisplayNameFilter, category, null,
                onlySelectedAttributes, considerInternalAttributes);
    }

    public List<LeadEnrichmentAttribute> getLeadEnrichmentAttributes(CustomerSpace customerSpace, //
                                                                     String attributeDisplayNameFilter, Category category, String subcategory, //
                                                                     Boolean onlySelectedAttributes, Boolean considerInternalAttributes) {
        return getLeadEnrichmentAttributes(customerSpace, attributeDisplayNameFilter, category, subcategory,
                onlySelectedAttributes, null, null, considerInternalAttributes);
    }

    @Override
    public List<LeadEnrichmentAttribute> getLeadEnrichmentAttributes(CustomerSpace customerSpace, //
                                                                     String attributeDisplayNameFilter, Category category, String subcategory, //
                                                                     Boolean onlySelectedAttributes, Integer offset, Integer max, Boolean considerInternalAttributes) {
        try {
            String url = constructUrl(combine(LATTICE_INSIGHTS_INTERNAL_ENRICHMENT + INSIGHTS_PATH + "", customerSpace.toString()));
            url = augumentEnrichmentAttributesUrl(url, attributeDisplayNameFilter, category, subcategory,
                    onlySelectedAttributes, offset, max, considerInternalAttributes);
            if (log.isDebugEnabled()) {
                log.debug("Get from " + url);
            }
            List<?> combinedAttributeObjList = get("getLeadEnrichmentAttributes", url, List.class);
            List<LeadEnrichmentAttribute> attributeList = JsonUtils.convertList(combinedAttributeObjList,
                    LeadEnrichmentAttribute.class);

            return attributeList;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[]{e.getMessage()});
        }
    }

    private String augumentEnrichmentAttributesUrl(String url, String attributeDisplayNameFilter, Category category,
                                                   String subcategory, Boolean onlySelectedAttributes, Integer offset, Integer max,
                                                   Boolean considerInternalAttributes) {
        url += "?" + "onlySelectedAttributes" + "=" + String.valueOf(Boolean.TRUE.equals(onlySelectedAttributes));
        if (!StringUtils.isEmpty(attributeDisplayNameFilter)) {
            url += "&" + "attributeDisplayNameFilter" + "=" + attributeDisplayNameFilter;
        }
        if (category != null) {
            url += "&" + "category" + "=" + category.toString();
            if (!StringUtils.isEmpty(subcategory)) {
                url += "&" + "subcategory" + "=" + subcategory.trim();
            }
        }
        if (offset != null) {
            url += "&" + "offset" + "=" + offset;
        }
        if (max != null) {
            url += "&" + "max" + "=" + max;
        }

        if (considerInternalAttributes) {
            url += "&" + "considerInternalAttributes" + "=" + considerInternalAttributes;
        }

        return url;
    }

}
