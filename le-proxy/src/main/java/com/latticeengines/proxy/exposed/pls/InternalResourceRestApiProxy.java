package com.latticeengines.proxy.exposed.pls;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.RemoteLedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.AdditionalEmailInfo;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport.Status;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.workflow.Job;
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

    //fix it: only for OrphanRecordExportDeploymentTestNG, after segmentExport migration, remove it.
    public MetadataSegmentExport createOrphanRecordThruMgr(MetadataSegmentExport metadataSegmentExport, CustomerSpace customerSpace){
        String url = constructUrl("pls/internal/segment/orphan/customerspace/" + customerSpace.toString());
        return restTemplate.postForObject(url, metadataSegmentExport, MetadataSegmentExport.class);
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
        if (exportId == null) {
            log.info("Skipping updating Metadata Segment Export as exportId is null");
            return null;
        }
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

    public List<Job> findJobsBasedOnActionIdsAndType(@NonNull String customerSpace, List<Long> actionPids,
            ActionType actionType) {
        try {
            if (CollectionUtils.isEmpty(actionPids)) {
                return Collections.emptyList();
            }
            String url = generateFindJobsBasedOnActionIdsAndTypeUrl(customerSpace, actionPids, actionType);
            List<?> listObj = restTemplate.getForObject(url, List.class);
            return JsonUtils.convertList(listObj, Job.class);
        } catch (Exception e) {
            throw new RuntimeException("findJobsBasedOnActionIdsAndType: Remote call failure: " + e.getMessage(), e);
        }
    }

    public void sendS3ImportEmail(String result, String tenantId, AdditionalEmailInfo emailInfo) {
        try {
            String url = constructUrl("pls/internal/emails/s3import/result", result, tenantId);
            log.info(String.format("Putting to %s", url));
            restTemplate.put(url, emailInfo);
        } catch (Exception e) {
            throw new RuntimeException("sendS3ImportEmail: Remote call failure", e);
        }
    }

    public void sendS3TemplateUpdateEmail(String tenantId, S3ImportEmailInfo emailInfo) {
        try {
            String url = constructUrl("pls/internal/emails/s3template/update", tenantId);
            log.info(String.format("Putting to %s", url));
            restTemplate.put(url, emailInfo);
        } catch (Exception e) {
            throw new RuntimeException("sendS3TemplateUpdateEmail: Remote call failure", e);
        }
    }

    public void sendS3TemplateCreateEmail(String tenantId, S3ImportEmailInfo emailInfo) {
        try {
            String url = constructUrl("pls/internal/emails/s3template/create", tenantId);
            log.info(String.format("Putting to %s", url));
            restTemplate.put(url, emailInfo);
        } catch (Exception e) {
            throw new RuntimeException("sendS3TemplateCreateEmail: Remote call failure", e);
        }
    }

    public ScoringRequestConfigContext retrieveScoringRequestConfigContext(String configUuid) {
        try {
            String url = constructUrl(
                    "pls/internal/external-scoring-config-context/" + configUuid);
            if (log.isDebugEnabled()) {
                log.debug("Find ScoringRequestConfigContext by configId (" + configUuid + ")" + url);
            }
            return restTemplate.getForObject(url, ScoringRequestConfigContext.class);
        } catch (RemoteLedpException rle) {
            throw rle;
        } catch (Exception e) {
            throw new RuntimeException("retrieveScoringRequestConfigContext: Remote call failure: " + e.getMessage(), e);
        }
    }
    
    @VisibleForTesting
    String generateFindJobsBasedOnActionIdsAndTypeUrl(String customerSpace, List<Long> actionPids,
            ActionType actionType) {
        StringBuilder urlStr = new StringBuilder();
        urlStr.append("pls/internal/jobs/all/").append(CustomerSpace.parse(customerSpace).toString());
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
}
