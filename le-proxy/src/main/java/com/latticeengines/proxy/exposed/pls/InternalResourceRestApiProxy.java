package com.latticeengines.proxy.exposed.pls;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.proxy.exposed.DeprecatedBaseRestApiProxy;

/*
 * this suppress warning is for DeprecatedBaseRestApiProxy class
 * the plan is to remove all the proxy that uses DeprecatedBaseRestApiProxy in the future,
 * so suppress for now.
 */
@SuppressWarnings("deprecation")
public class InternalResourceRestApiProxy extends DeprecatedBaseRestApiProxy {

    private static final Logger log = LoggerFactory.getLogger(InternalResourceRestApiProxy.class);

    private static final String INSIGHTS_PATH = "/insights";

    private static final String PLS_INTERNAL_ENRICHMENT = "pls/internal/enrichment";

    private String internalResourceHostPort;

    public InternalResourceRestApiProxy(String internalResourceHostPort) {
        super();
        this.internalResourceHostPort = internalResourceHostPort;
    }

    @Override
    public String getRestApiHostPort() {
        return internalResourceHostPort;
    }

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

    public List<LeadEnrichmentAttribute> getLeadEnrichmentAttributes(CustomerSpace customerSpace, //
                                                                     String attributeDisplayNameFilter, Category category, String subcategory, //
                                                                     Boolean onlySelectedAttributes, Integer offset, Integer max, Boolean considerInternalAttributes) {
        try {
            String url = constructUrl(PLS_INTERNAL_ENRICHMENT + INSIGHTS_PATH + "", customerSpace.toString());
            url = augumentEnrichmentAttributesUrl(url, attributeDisplayNameFilter, category, subcategory,
                    onlySelectedAttributes, offset, max, considerInternalAttributes);
            if (log.isDebugEnabled()) {
                log.debug("Get from " + url);
            }
            List<?> combinedAttributeObjList = restTemplate.getForObject(url, List.class);
            List<LeadEnrichmentAttribute> attributeList = JsonUtils.convertList(combinedAttributeObjList,
                    LeadEnrichmentAttribute.class);

            return attributeList;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[] { e.getMessage() });
        }
    }

    public int getLeadEnrichmentAttributesCount(CustomerSpace customerSpace, String attributeDisplayNameFilter,
                                                Category category, String subcategory, Boolean onlySelectedAttributes, Boolean considerInternalAttributes) {
        try {
            String url = constructUrl(PLS_INTERNAL_ENRICHMENT + INSIGHTS_PATH + "/count", customerSpace.toString());
            url = augumentEnrichmentAttributesUrl(url, attributeDisplayNameFilter, category, subcategory,
                    onlySelectedAttributes, null, null, considerInternalAttributes);
            if (log.isDebugEnabled()) {
                log.debug("Get from " + url);
            }
            return restTemplate.getForObject(url, Integer.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[] { e.getMessage() });
        }

    }

    public List<LeadEnrichmentAttribute> getAllLeadEnrichmentAttributes() {
        try {
            String url = constructUrl(PLS_INTERNAL_ENRICHMENT + "/all" + INSIGHTS_PATH);

            log.debug("Get from " + url);
            List<?> combinedAttributeObjList = restTemplate.getForObject(url, List.class);
            List<LeadEnrichmentAttribute> attributeList = JsonUtils.convertList(combinedAttributeObjList,
                    LeadEnrichmentAttribute.class);

            return attributeList;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[] { e.getMessage() });
        }
    }

    public void saveLeadEnrichmentAttributes(CustomerSpace customerSpace, //
                                             LeadEnrichmentAttributesOperationMap attributes) {
        try {
            String url = constructUrl(PLS_INTERNAL_ENRICHMENT + INSIGHTS_PATH, customerSpace.toString());
            restTemplate.put(url, attributes);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[] { e.getMessage() });
        }
    }

    public Map<String, Integer> getPremiumAttributesLimitation(CustomerSpace customerSpace) {
        try {
            String url = constructUrl(PLS_INTERNAL_ENRICHMENT + INSIGHTS_PATH + "/premiumattributeslimitation",
                    customerSpace.toString());

            Map<?, ?> limitationMap = restTemplate.getForObject(url, Map.class);

            Map<String, Integer> premiumAttributesLimitationMap = new HashMap<>();

            if (!MapUtils.isEmpty(limitationMap)) {
                premiumAttributesLimitationMap = JsonUtils.convertMap(limitationMap, String.class, Integer.class);
            }

            return premiumAttributesLimitationMap;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[] { e.getMessage() });
        }
    }

    public Integer getSelectedAttributeCount(CustomerSpace customerSpace, Boolean considerInternalAttributes) {
        try {
            String url = constructUrl(PLS_INTERNAL_ENRICHMENT + INSIGHTS_PATH + "/selectedattributes/count",
                    customerSpace.toString());
            if (considerInternalAttributes) {
                url += "?" + "considerInternalAttributes" + "=" + considerInternalAttributes;
            }
            return restTemplate.getForObject(url, Integer.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[] { e.getMessage() });
        }
    }

    public Integer getSelectedAttributePremiumCount(CustomerSpace customerSpace, Boolean considerInternalAttributes) {
        try {
            String url = constructUrl(PLS_INTERNAL_ENRICHMENT + INSIGHTS_PATH + "/selectedpremiumattributes/count",
                    customerSpace.toString());
            if (considerInternalAttributes) {
                url += "?" + "considerInternalAttributes" + "=" + considerInternalAttributes;
            }
            return restTemplate.getForObject(url, Integer.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[] { e.getMessage() });
        }
    }

    public List<String> getLeadEnrichmentCategories(CustomerSpace customerSpace) {
        try {
            String url = constructUrl(PLS_INTERNAL_ENRICHMENT + INSIGHTS_PATH + "/categories",
                    customerSpace.toString());
            List<?> categoriesObjList = restTemplate.getForObject(url, List.class);
            return JsonUtils.convertList(categoriesObjList, String.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[] { e.getMessage() });
        }
    }

    public List<String> getLeadEnrichmentSubcategories(CustomerSpace customerSpace, String category) {
        try {
            String url = constructUrl(PLS_INTERNAL_ENRICHMENT + INSIGHTS_PATH + "/subcategories",
                    customerSpace.toString());
            url += "?category=" + category;
            List<?> subCategoriesObjList = restTemplate.getForObject(url, List.class);
            return JsonUtils.convertList(subCategoriesObjList, String.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[] { e.getMessage() });
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getActiveStack() {
        try {
            String url = constructUrl("pls/health/stackinfo");
            return restTemplate.getForObject(url, Map.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[] { e.getMessage() });
        }
    }

    public StatusDocument systemCheck() {
        try {
            String url = constructUrl("pls/health/systemstatus");
            return restTemplate.getForObject(url, StatusDocument.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31115, new String[] { e.getMessage() });
        }
    }

    public void createSourceFile(SourceFile sourceFile, String tenantId) {
        try {
            String url = constructUrl("pls/internal/sourcefiles", sourceFile.getName(), tenantId);
            log.info(String.format("Posting to %s", url));
            restTemplate.postForObject(url, sourceFile, Void.class);
        } catch (Exception e) {
            throw new RuntimeException("createSourceFile: Remote call failure", e);
        }
    }

    public SourceFile findSourceFileByName(String name, String tenantId) {
        try {
            String url = constructUrl("pls/internal/sourcefiles", name, tenantId);
            log.info(String.format("Getting from %s", url));
            return restTemplate.getForObject(url, SourceFile.class);
        } catch (Exception e) {
            throw new RuntimeException("findSourceFileByName: Remote call failure", e);
        }
    }

    public void updateSourceFile(SourceFile sourceFile, String tenantId) {
        try {
            String url = constructUrl("pls/internal/sourcefiles", sourceFile.getName(), tenantId);
            log.info(String.format("Putting to %s", url));
            restTemplate.put(url, sourceFile);
        } catch (Exception e) {
            throw new RuntimeException("updateSourceFile: Remote call failure", e);
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

    public Report findReportByName(CustomerSpace customerSpace, String reportName) {
        try {
            String url = constructUrl("pls/internal/reports/" + reportName + "/" + customerSpace.toString());
            return restTemplate.getForObject(url, Report.class);
        } catch (Exception e) {
            throw new RuntimeException("getReport: Remote call failure: " + e.getMessage(), e);
        }
    }
}
