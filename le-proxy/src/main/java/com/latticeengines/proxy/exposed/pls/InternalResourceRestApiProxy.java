package com.latticeengines.proxy.exposed.pls;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.util.BaseRestApiProxy;

public class InternalResourceRestApiProxy extends BaseRestApiProxy {

    private static final String LOOKUP_ID_DELIM = "|";

    private static final Log log = LogFactory.getLog(InternalResourceRestApiProxy.class);

    private String internalResourceHostPort;

    public InternalResourceRestApiProxy(String internalResourceHostPort) {
        super();
        this.internalResourceHostPort = internalResourceHostPort;
    }

    @Override
    public String getRestApiHostPort() {
        return internalResourceHostPort;
    }

    public List<?> getActiveModelSummaries(CustomerSpace customerSpace) {
        try {
            String url = constructUrl("pls/internal/modelsummaries/active", customerSpace.toString());
            log.debug("Get from " + url);
            return restTemplate.getForObject(url, List.class);
        } catch (Exception e) {
            throw new RuntimeException("getActiveModelSummaries: Remote call failure", e);
        }
    }

    public ModelSummary getModelSummaryFromModelId(String modelId, CustomerSpace customerSpace) {
        ModelSummary modelSummary = null;
        try {
            String url = constructUrl("pls/internal/modelsummaries/modelid", modelId, customerSpace.toString());
            log.debug("Get from " + url);
            modelSummary = restTemplate.getForObject(url, ModelSummary.class);
            if (modelSummary != null && StringUtils.isEmpty(modelSummary.getEventTableName())) {
                String lookupId = modelSummary.getLookupId();
                String eventTableName = lookupId.substring(lookupId.indexOf(LOOKUP_ID_DELIM) + 1,
                        lookupId.lastIndexOf(LOOKUP_ID_DELIM));
                modelSummary.setEventTableName(eventTableName);
            }
        } catch (Exception e) {
            throw new RuntimeException("getModelSummaryFromModelId: Remote call failure", e);
        }
        return modelSummary;
    }

    public List<String> getRequiredColumnNames(String modelId, CustomerSpace customerSpace) {
        String url = constructUrl("pls/internal/metadata/required/modelId/", modelId, customerSpace.toString());
        List<?> requiredColumnObjList = restTemplate.getForObject(url, List.class);
        return JsonUtils.convertList(requiredColumnObjList, String.class);
    }

    public void createModelSummary(ModelSummary modelSummary, CustomerSpace customerSpace) {
        try {
            String url = constructUrl("pls/internal/modelsummaries", customerSpace.toString());
            log.debug(String.format("Posting to %s", url));
            restTemplate.postForObject(url, modelSummary, Void.class);
        } catch (Exception e) {
            throw new RuntimeException("createModelSummary: Remote call failure", e);
        }
    }

    public void deleteModelSummary(String modelId, CustomerSpace customerSpace) {
        try {
            String url = constructUrl("pls/internal/modelsummaries/", modelId, customerSpace.toString());
            log.debug(String.format("Deleting to %s", url));
            restTemplate.delete(url);
        } catch (Exception e) {
            throw new RuntimeException("deleteModelSummary: Remote call failure", e);
        }
    }

    public boolean createTenant(Tenant tenant) {
        try {
            String url = constructUrl("pls/admin/tenants");
            log.debug(String.format("Posting to %s", url));
            return restTemplate.postForObject(url, tenant, Boolean.class);
        } catch (Exception e) {
            throw new RuntimeException("createTenant: Remote call failure", e);
        }
    }

    public void deleteTenant(CustomerSpace customerSpace) {
        try {
            String url = constructUrl("pls/admin/tenants/", customerSpace.toString());
            log.debug(String.format("Deleting to %s", url));
            restTemplate.delete(url);
        } catch (Exception e) {
            throw new RuntimeException("deleteTenant: Remote call failure", e);
        }
    }

    public int getModelsCount(CustomerSpace customerSpace, String start, boolean considerAllStatus) {
        try {
            String url = constructUrl("pls/internal/modelsummarydetails/count", customerSpace.toString());
            url += "?" + "considerAllStatus" + "=" + considerAllStatus;
            if (!StringUtils.isEmpty(start)) {
                url += "&" + "start" + "=" + start;
            }
            log.debug("Get from " + url);
            return restTemplate.getForObject(url, Integer.class);
        } catch (Exception e) {
            throw new RuntimeException("getModelsCount: Remote call failure: " + e.getMessage(), e);
        }
    }

    public List<ModelSummary> getPaginatedModels(CustomerSpace customerSpace, String start, int offset, int maximum,
            boolean considerAllStatus) {
        try {
            String url = constructUrl("pls/internal/modelsummarydetails/paginate", customerSpace.toString());
            url += "?" + "considerAllStatus" + "=" + considerAllStatus + "&" + "offset" + "=" + offset + "&" + "maximum"
                    + "=" + maximum;
            if (!StringUtils.isEmpty(start)) {
                url += "&" + "start" + "=" + start;
            }

            log.debug("Get from " + url);
            List<?> modelSummaryObjList = restTemplate.getForObject(url, List.class);
            List<ModelSummary> modelSummaryList = JsonUtils.convertList(modelSummaryObjList, ModelSummary.class);

            return modelSummaryList;
        } catch (Exception e) {
            throw new RuntimeException("getPaginatedModels: Remote call failure: " + e.getMessage(), e);
        }
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
            String url = constructUrl("pls/internal/enrichment/lead", customerSpace.toString());
            url = augumentEnrichmentAttributesUrl(url, attributeDisplayNameFilter, category, subcategory,
                    onlySelectedAttributes, offset, max, considerInternalAttributes);

            log.debug("Get from " + url);
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
            String url = constructUrl("pls/internal/enrichment/lead/count", customerSpace.toString());
            url = augumentEnrichmentAttributesUrl(url, attributeDisplayNameFilter, category, subcategory,
                    onlySelectedAttributes, null, null, considerInternalAttributes);

            log.debug("Get from " + url);
            return restTemplate.getForObject(url, Integer.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[] { e.getMessage() });
        }

    }

    public List<LeadEnrichmentAttribute> getAllLeadEnrichmentAttributes() {
        try {
            String url = constructUrl("pls/internal/enrichment/all/lead");

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
            String url = constructUrl("pls/internal/enrichment/lead", customerSpace.toString());
            restTemplate.put(url, attributes);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[] { e.getMessage() });
        }
    }

    public Map<String, Integer> getPremiumAttributesLimitation(CustomerSpace customerSpace) {
        try {
            String url = constructUrl("pls/internal/enrichment/lead/premiumattributeslimitation",
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

    public Integer getSelectedAttributeCount(CustomerSpace customerSpace) {
        try {
            String url = constructUrl("pls/internal/enrichment/lead/selectedattributes/count",
                    customerSpace.toString());
            return restTemplate.getForObject(url, Integer.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[] { e.getMessage() });
        }
    }

    public Integer getSelectedAttributePremiumCount(CustomerSpace customerSpace) {
        try {
            String url = constructUrl("pls/internal/enrichment/lead/selectedpremiumattributes/count",
                    customerSpace.toString());
            return restTemplate.getForObject(url, Integer.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[] { e.getMessage() });
        }
    }

    public List<String> getLeadEnrichmentCategories(CustomerSpace customerSpace) {
        try {
            String url = constructUrl("pls/internal/enrichment/lead/categories", customerSpace.toString());
            List<?> categoriesObjList = restTemplate.getForObject(url, List.class);
            return JsonUtils.convertList(categoriesObjList, String.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[] { e.getMessage() });
        }
    }

    public List<String> getLeadEnrichmentSubcategories(CustomerSpace customerSpace, String category) {
        try {
            String url = constructUrl("pls/internal/enrichment/lead/subcategories", customerSpace.toString());
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
            String url = constructUrl("pls/internal/currentstack");
            return restTemplate.getForObject(url, Map.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[] { e.getMessage() });
        }
    }

    private String augumentEnrichmentAttributesUrl(String url, String attributeDisplayNameFilter, Category category,
            String subcategory, Boolean onlySelectedAttributes, Integer offset, Integer max,
            Boolean considerInternalAttributes) {
        url += "?" + "onlySelectedAttributes" + "="
                + ((onlySelectedAttributes != null && onlySelectedAttributes == true) ? true : false);
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
            url += "&" + "offset" + "=" + offset.intValue();
        }
        if (max != null) {
            url += "&" + "max" + "=" + max.intValue();
        }

        if (considerInternalAttributes) {
            url += "&" + "considerInternalAttributes" + "=" + considerInternalAttributes;
        }

        return url;
    }
}