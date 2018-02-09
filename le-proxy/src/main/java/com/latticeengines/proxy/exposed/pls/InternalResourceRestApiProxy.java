package com.latticeengines.proxy.exposed.pls;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.lang.NonNull;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport.Status;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.proxy.exposed.DeprecatedBaseRestApiProxy;

public class InternalResourceRestApiProxy extends DeprecatedBaseRestApiProxy {

    private static final String LOOKUP_ID_DELIM = "|";

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

    public List<ModelSummary> getModelSummariesModifiedWithinTimeFrame(long timeFrame) {
        try {
            String url = constructUrl("pls/internal/modelsummaries/updated", String.valueOf(timeFrame));
            log.debug("Get from " + url);
            List<?> modelSummaryObjList = restTemplate.getForObject(url, List.class);
            List<ModelSummary> modelSummaryList = JsonUtils.convertList(modelSummaryObjList, ModelSummary.class);
            return modelSummaryList;
        } catch (Exception e) {
            throw new RuntimeException("getModelSummariesNeedToRefreshInCache: Remote call failure", e);
        }
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
            log.info("getting models count from " + url);

            URI uri = restTemplate.getUriTemplateHandler().expand(url);
            log.info("Encoded URL: " + uri);

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
            log.info("getting paginated models from " + url);

            URI uri = restTemplate.getUriTemplateHandler().expand(url);
            log.info("Encoded URL: " + uri);

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
            String url = constructUrl(PLS_INTERNAL_ENRICHMENT + INSIGHTS_PATH + "", customerSpace.toString());
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
            String url = constructUrl(PLS_INTERNAL_ENRICHMENT + INSIGHTS_PATH + "/count", customerSpace.toString());
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

    public List<BucketMetadata> getUpToDateABCDBuckets(String modelId, CustomerSpace customerSpace) {
        try {
            String url = constructUrl("pls/internal/abcdbuckets/uptodate", modelId);
            url += "?tenantId=" + customerSpace.toString();
            List<?> bucketMetadataList = restTemplate.getForObject(url, List.class);
            return JsonUtils.convertList(bucketMetadataList, BucketMetadata.class);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Remote call failure for getting the up-to-date bucekts of the model %s of tenant %s",
                            modelId, customerSpace.toString()),
                    e);
        }
    }

    public void createABCDBuckets(String modelId, CustomerSpace customerSpace,
            List<BucketMetadata> bucketMetadataList) {
        try {
            String url = constructUrl("pls/internal/abcdbuckets/", modelId);
            url += "?tenantId=" + customerSpace.toString();
            log.debug(String.format("Posting to %s", url));
            restTemplate.postForEntity(url, bucketMetadataList, Void.class);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Remote call failure for creating abcd buckets for model %s of tenant %s", modelId,
                            customerSpace.toString()),
                    e);
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

    public List<BucketMetadata> createDefaultABCDBuckets(String modelId, String userId, boolean cdl,
            boolean expectedValue, boolean liftChart) {
        try {
            String url = constructUrl("pls/internal/bucketmetadata", modelId);
            url = url + String.format("?cdl=%b&expectedValue=%b&liftChart=%b", cdl, expectedValue, liftChart);
            List<?> abcdBuckets = restTemplate.postForObject(url, userId, List.class);
            return JsonUtils.convertList(abcdBuckets, BucketMetadata.class);
        } catch (Exception e) {
            throw new RuntimeException("create default abcd buckets: Remote call failure", e);
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

    public TargetMarket findTargetMarketByName(String targetMarketName, String tenantId) {
        try {
            return restTemplate.getForObject(constructUrl("pls/internal/targetmarkets/", targetMarketName, tenantId),
                    TargetMarket.class);
        } catch (Exception e) {
            throw new RuntimeException("findTargetMarketByName: Remote call failure", e);
        }
    }

    public void deleteTargetMarketByName(String targetMarketName, String tenantId) {
        try {
            restTemplate.delete(constructUrl("pls/internal/targetmarkets/", targetMarketName, tenantId));
        } catch (Exception e) {
            throw new RuntimeException("deleteTargetMarketByName: Remote call failure", e);
        }
    }

    public TargetMarket createDefaultTargetMarket(String tenantId) {
        try {
            return restTemplate.postForObject(constructUrl("pls/internal/targetmarkets/default", tenantId), null,
                    TargetMarket.class);
        } catch (Exception e) {
            throw new RuntimeException("createDefaultTargetMarket: Remote call failure", e);
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

    public void activateModelSummary(String modelId) {
        try {
            String url = constructUrl("pls/internal/modelsummaries", modelId);
            log.info(String.format("Putting to %s", url));
            AttributeMap attrMap = new AttributeMap();
            attrMap.put(ModelSummary.STATUS, ModelSummaryStatus.ACTIVE.getStatusCode());
            HttpEntity<AttributeMap> requestEntity = new HttpEntity<>(attrMap);
            restTemplate.exchange(url, HttpMethod.PUT, requestEntity, Object.class);
        } catch (Exception e) {
            throw new RuntimeException("activate model summary: Remote call failure", e);
        }
    }

    public void setModelSummaryDownloadFlag(String tenantId) {
        try {
            String url = constructUrl("pls/internal/modelsummarydownloadflag", tenantId);
            log.info(String.format("Posting to %s", url));
            restTemplate.exchange(url, HttpMethod.POST, null, Object.class);
        } catch (Exception e) {
            throw new RuntimeException("set model summary flag: Remote call failure", e);
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

    public MetadataSegmentExport getMetadataSegmentExport(CustomerSpace customerSpace, //
            String exportId) {
        try {
            String url = constructUrl("pls/internal/segment/export/" + exportId + "/" + customerSpace.toString());
            log.debug("Find MetadataSegmentExport by exportId (" + exportId + ")" + url);
            return restTemplate.getForObject(url, MetadataSegmentExport.class);
        } catch (Exception e) {
            throw new RuntimeException("getMetadataSegmentExport: Remote call failure: " + e.getMessage(), e);
        }
    }

    public MetadataSegmentExport updateMetadataSegmentExport(CustomerSpace customerSpace, //
            String exportId, Status state) {
        try {
            String url = constructUrl("pls/internal/segment/export/" + exportId + "/" + customerSpace.toString());
            url += "?" + "state=" + state;

            log.debug("Update MetadataSegmentExport by exportId (" + exportId + ")" + url);
            restTemplate.put(url, null);
            return getMetadataSegmentExport(customerSpace, exportId);
        } catch (Exception e) {
            throw new RuntimeException("updateMetadataSegmentExport: Remote call failure: " + e.getMessage(), e);
        }
    }

    public Restriction getSegmentRestrictionQuery(CustomerSpace customerSpace, String segmentName) {
        String url = constructUrl("pls/internal/segment/" + segmentName + "/restriction", customerSpace.toString());
        return restTemplate.getForObject(url, Restriction.class);
    }

    public Report findReportByName(CustomerSpace customerSpace, String reportName) {
        try {
            String url = constructUrl("pls/internal/reports/" + reportName + "/" + customerSpace.toString());
            return restTemplate.getForObject(url, Report.class);
        } catch (Exception e) {
            throw new RuntimeException("getReport: Remote call failure: " + e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    public List<ColumnMetadata> getAttributesInPredefinedGroup(ColumnSelection.Predefined predefined,
            String customerSpace) {
        try {
            String url = constructUrl(
                    "pls/internal/datacollection/attributes/" + predefined.getName() + "/" + customerSpace);
            return restTemplate.getForObject(url, List.class);
        } catch (Exception e) {
            throw new RuntimeException("getAttributesInPredefinedGroup: Remote call failure: " + e.getMessage(), e);
        }
    }

    public List<Action> findAllActions(String customerSpace) {
        try {
            String url = constructUrl("pls/internal/actions/all/" + customerSpace);
            List<?> listObj = restTemplate.getForObject(url, List.class);
            return JsonUtils.convertList(listObj, Action.class);
        } catch (Exception e) {
            throw new RuntimeException("getAllActions: Remote call failure: " + e.getMessage(), e);
        }
    }

    public List<Action> findByPidIn(String customerSpace, @NonNull List<Long> pids) {
        try {
            String url = generateGetAllActionsByPidsUrl(customerSpace, pids);
            List<?> listObj = restTemplate.getForObject(url, List.class);
            return JsonUtils.convertList(listObj, Action.class);
        } catch (Exception e) {
            throw new RuntimeException("getAllActionsByPids: Remote call failure: " + e.getMessage(), e);
        }
    }

    private String generateGetAllActionsByPidsUrl(String customerSpace, List<Long> pids) {
        StringBuilder urlStr = new StringBuilder();
        urlStr.append("pls/internal/actions/all/").append(customerSpace).append("?");
        for (Long pid : pids) {
            urlStr.append(String.format("pid=%s&", pid));
        }
        urlStr.setLength(urlStr.length() - 1);
        return constructUrl(urlStr.toString());
    }

    public List<Action> getActionsByOwnerId(String customerSpace, Long ownerId) {
        try {
            String url = constructUrl("pls/internal/actions/ownerid/" + ownerId + "/" + customerSpace);
            List<?> listObj = restTemplate.getForObject(url, List.class);
            return JsonUtils.convertList(listObj, Action.class);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("getActionsByOnwerId for ownerId=%s: Remote call failure: ", ownerId)
                            + e.getMessage(),
                    e);
        }
    }

    public Action createAction(String customerSpace, Action action) {
        try {
            String url = constructUrl("pls/internal/actions/" + customerSpace);
            return restTemplate.postForObject(url, action, Action.class);
        } catch (Exception e) {
            throw new RuntimeException("createAction: Remote call failure: " + e.getMessage(), e);
        }
    }

    public void updateAction(String customerSpace, Action action) {
        try {
            String url = constructUrl("pls/internal/actions/" + customerSpace);
            restTemplate.put(url, action);
        } catch (Exception e) {
            throw new RuntimeException("updateAction: Remote call failure: " + e.getMessage(), e);
        }
    }

    public void updateOwnerIdIn(String customerSpace, @NonNull Long ownerId, @NonNull List<Long> pids) {
        try {
            restTemplate.patchForObject(generatePatchOwnerIdOfActionsUrl(customerSpace, ownerId, pids), null,
                    Void.class);
        } catch (Exception e) {
            throw new RuntimeException("patchOwnerIdOfActions: Remote call failure: " + e.getMessage(), e);
        }
    }

    private String generatePatchOwnerIdOfActionsUrl(String customerSpace, Long ownerId, List<Long> pids) {
        StringBuilder urlStr = new StringBuilder();
        urlStr.append("pls/internal/actions/").append(customerSpace).append("?");
        for (Long pid : pids) {
            urlStr.append(String.format("pid=%s&", pid));
        }
        urlStr.append(String.format("ownerId=%s", ownerId));
        return constructUrl(urlStr.toString());
    }

    public void deleteAction(String customerSpace, @NonNull Long pid) {
        try {
            String url = constructUrl("pls/internal/actions/" + pid + "/" + customerSpace);
            restTemplate.delete(url);
        } catch (Exception e) {
            throw new RuntimeException("deleteAction: Remote call failure: " + e.getMessage(), e);
        }
    }

    public List<Job> findJobsBasedOnActionIdsAndType(@NonNull String customerSpace, List<Long> actionPids,
            ActionType actionType) {
        try {
            String url = generateFindJobsBasedOnActionIdsAndTypeUrl(customerSpace, actionPids, actionType);
            List<?> listObj = restTemplate.getForObject(url, List.class);
            return JsonUtils.convertList(listObj, Job.class);
        } catch (Exception e) {
            throw new RuntimeException("findJobsBasedOnActionIdsAndType: Remote call failure: " + e.getMessage(), e);
        }
    }

    @VisibleForTesting
    String generateFindJobsBasedOnActionIdsAndTypeUrl(String customerSpace, List<Long> actionPids,
            ActionType actionType) {
        StringBuilder urlStr = new StringBuilder();
        urlStr.append("pls/internal/jobs/all/").append(shortenCustomerSpace(customerSpace));
        if (CollectionUtils.isNotEmpty(actionPids) || actionType != null) {
            urlStr.append("?");
            if (CollectionUtils.isNotEmpty(actionPids)) {
                for (Long pid : actionPids) {
                    urlStr.append(String.format("pid=%s&", pid));
                }
            }
            if (actionType != null) {
                urlStr.append(String.format("type=%s", actionType));
            }
        }
        if (urlStr.charAt(urlStr.length() - 1) == '&') {
            urlStr.setLength(urlStr.length() - 1);
        }
        return constructUrl(urlStr.toString());
    }

    public int getInvokeTime(@NotNull CustomerSpace customerSpace) {
        try {
            String url = constructUrl("pls/internal/plscomponent/invoketime", customerSpace.toString());
            return restTemplate.getForObject(url, Integer.class);
        } catch (Exception e) {
            throw new RuntimeException("getInvokeTime: Remote call failure: " + e.getMessage(), e);
        }
    }
}
