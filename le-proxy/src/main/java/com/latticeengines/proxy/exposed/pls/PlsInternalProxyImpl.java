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
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsExportRequest;
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
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

/*
 * this suppress warning is for DeprecatedBaseRestApiProxy class
 * the plan is to remove all the proxy that uses DeprecatedBaseRestApiProxy in the future,
 * so suppress for now.
 */
@Component("plsInterProxy")
public class PlsInternalProxyImpl extends BaseRestApiProxy implements PlsInternalProxy {

    private static final Logger log = LoggerFactory.getLogger(PlsInternalProxyImpl.class);

    private static final String INSIGHTS_PATH = "/insights";

    private static final String PLS_INTERNAL_ENRICHMENT = "/internal/enrichment";

    public PlsInternalProxyImpl() {
        super(PropertyUtils.getProperty("common.pls.url"), "pls");
    }


    public PlsInternalProxyImpl(String hostPort) {
        super(hostPort, "pls");
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
            String url = constructUrl(combine(PLS_INTERNAL_ENRICHMENT + INSIGHTS_PATH + "", customerSpace.toString()));
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

    @Override
    public int getLeadEnrichmentAttributesCount(CustomerSpace customerSpace, String attributeDisplayNameFilter,
                                                Category category, String subcategory, Boolean onlySelectedAttributes, Boolean considerInternalAttributes) {
        try {
            String url = constructUrl(combine(PLS_INTERNAL_ENRICHMENT + INSIGHTS_PATH + "/count",
                    customerSpace.toString()));
            url = augumentEnrichmentAttributesUrl(url, attributeDisplayNameFilter, category, subcategory,
                    onlySelectedAttributes, null, null, considerInternalAttributes);
            if (log.isDebugEnabled()) {
                log.debug("Get from " + url);
            }
            return get("getLeadEnrichmentAttributesCount", url, Integer.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[]{e.getMessage()});
        }
    }

    @Override
    public List<LeadEnrichmentAttribute> getAllLeadEnrichmentAttributes() {
        try {
            String url = constructUrl(combine(PLS_INTERNAL_ENRICHMENT + "/all" + INSIGHTS_PATH));
            log.debug("Get from " + url);
            List<?> combinedAttributeObjList = get("getAllLeadEnrichmentAttributes", url, List.class);
            List<LeadEnrichmentAttribute> attributeList = JsonUtils.convertList(combinedAttributeObjList,
                    LeadEnrichmentAttribute.class);
            return attributeList;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[]{e.getMessage()});
        }
    }

    @Override
    public void saveLeadEnrichmentAttributes(CustomerSpace customerSpace, //
                                             LeadEnrichmentAttributesOperationMap attributes) {
        try {
            String url = constructUrl(combine(PLS_INTERNAL_ENRICHMENT + INSIGHTS_PATH, customerSpace.toString()));
            put("saveLeadEnrichmentAttributes", url, attributes);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[]{e.getMessage()});
        }
    }

    @Override
    public Map<String, Integer> getPremiumAttributesLimitation(CustomerSpace customerSpace) {
        try {
            String url = constructUrl(combine(PLS_INTERNAL_ENRICHMENT + INSIGHTS_PATH + "/premiumattributeslimitation",
                    customerSpace.toString()));

            Map<?, ?> limitationMap = get("getPremiumAttributesLimitation", url, Map.class);

            Map<String, Integer> premiumAttributesLimitationMap = new HashMap<>();

            if (!MapUtils.isEmpty(limitationMap)) {
                premiumAttributesLimitationMap = JsonUtils.convertMap(limitationMap, String.class, Integer.class);
            }

            return premiumAttributesLimitationMap;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[]{e.getMessage()});
        }
    }

    @Override
    public Integer getSelectedAttributeCount(CustomerSpace customerSpace, Boolean considerInternalAttributes) {
        try {
            String url = constructUrl(combine(PLS_INTERNAL_ENRICHMENT + INSIGHTS_PATH + "/selectedattributes/count",
                    customerSpace.toString()));
            if (considerInternalAttributes) {
                url += "?" + "considerInternalAttributes" + "=" + considerInternalAttributes;
            }
            return get("getSelectedAttributeCount", url, Integer.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[]{e.getMessage()});
        }
    }

    @Override
    public Integer getSelectedAttributePremiumCount(CustomerSpace customerSpace, Boolean considerInternalAttributes) {
        try {
            String url = constructUrl(combine(PLS_INTERNAL_ENRICHMENT + INSIGHTS_PATH + "/selectedpremiumattributes/count",
                    customerSpace.toString()));
            if (considerInternalAttributes) {
                url += "?" + "considerInternalAttributes" + "=" + considerInternalAttributes;
            }
            return get("getSelectedAttributePremiumCount", url, Integer.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[]{e.getMessage()});
        }
    }

    @Override
    public List<String> getLeadEnrichmentCategories(CustomerSpace customerSpace) {
        try {
            String url = constructUrl(combine(PLS_INTERNAL_ENRICHMENT + INSIGHTS_PATH + "/categories",
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
            String url = constructUrl(combine(PLS_INTERNAL_ENRICHMENT + INSIGHTS_PATH + "/subcategories",
                    customerSpace.toString()));
            url += "?category=" + category;
            List<?> subCategoriesObjList = get("getLeadEnrichmentSubcategories", url, List.class);
            return JsonUtils.convertList(subCategoriesObjList, String.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, new String[]{e.getMessage()});
        }
    }

    @Override
    public void createSourceFile(SourceFile sourceFile, String tenantId) {
        try {
            String url = constructUrl(combine("/internal/sourcefiles", sourceFile.getName(), tenantId));
            log.info(String.format("Posting to %s", url));
            post("createSourceFile", url, sourceFile, Void.class);
        } catch (Exception e) {
            throw new RuntimeException("createSourceFile: Remote call failure", e);
        }
    }

    @Override
    public SourceFile findSourceFileByName(String name, String tenantId) {
        try {
            String url = constructUrl(combine("/internal/sourcefiles", name, tenantId));
            log.info(String.format("Getting from %s", url));
            return get("findSourceFileByName", url, SourceFile.class);
        } catch (Exception e) {
            throw new RuntimeException("findSourceFileByName: Remote call failure", e);
        }
    }

    @Override
    public void updateSourceFile(SourceFile sourceFile, String tenantId) {
        try {
            String url = constructUrl(combine("/internal/sourcefiles", sourceFile.getName(), tenantId));
            log.info(String.format("Putting to %s", url));
            put("updateSourceFile", url, sourceFile);
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

    @Override
    public MetadataSegmentExport getMetadataSegmentExport(CustomerSpace customerSpace, //
                                                          String exportId) {
        try {
            String url = constructUrl(combine("/internal/segment/export", exportId, customerSpace.toString()));
            log.debug("Find MetadataSegmentExport by exportId (" + exportId + ")" + url);
            return get("getMetadataSegmentExport", url, MetadataSegmentExport.class);
        } catch (Exception e) {
            throw new RuntimeException("getMetadataSegmentExport: Remote call failure: " + e.getMessage(), e);
        }
    }

    @Override
    public MetadataSegmentExport updateMetadataSegmentExport(CustomerSpace customerSpace, //
                                                             String exportId, Status state) {
        if (exportId == null) {
            log.info("Skipping updating Metadata Segment Export as exportId is null");
            return null;
        }
        try {
            String url = constructUrl(combine(combine("/internal/segment/export", exportId, customerSpace.toString())));
            url += "?" + "state=" + state;

            log.debug("Update MetadataSegmentExport by exportId (" + exportId + ")" + url);
            put("updateMetadataSegmentExport", url, null);
            return getMetadataSegmentExport(customerSpace, exportId);
        } catch (Exception e) {
            throw new RuntimeException("updateMetadataSegmentExport: Remote call failure: " + e.getMessage(), e);
        }
    }

    public Restriction getSegmentRestrictionQuery(CustomerSpace customerSpace, String segmentName) {
        String url = constructUrl(combine("/internal/segment/" + segmentName + "/restriction",
                customerSpace.toString()));
        return get("getSegmentRestrictionQuery", url, Restriction.class);
    }

    @Override
    public List<Job> findJobsBasedOnActionIdsAndType(@NonNull String customerSpace, List<Long> actionPids,
                                                     ActionType actionType) {
        try {
            if (CollectionUtils.isEmpty(actionPids)) {
                return Collections.emptyList();
            }
            String url = generateFindJobsBasedOnActionIdsAndTypeUrl(customerSpace, actionPids,
                    actionType);
            List<?> listObj = get("findJobsBasedOnActionIdsAndType", url, List.class);
            return JsonUtils.convertList(listObj, Job.class);
        } catch (Exception e) {
            throw new RuntimeException("findJobsBasedOnActionIdsAndType: Remote call failure: " + e.getMessage(), e);
        }
    }

    public void sendS3ImportEmail(String result, String tenantId, S3ImportEmailInfo emailInfo) {
        try {
            String url = constructUrl(combine("/internal/emails/s3import/result", result, tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendS3ImportEmail", url, emailInfo);
        } catch (Exception e) {
            throw new RuntimeException("sendS3ImportEmail: Remote call failure", e);
        }
    }

    public void sendS3TemplateUpdateEmail(String tenantId, S3ImportEmailInfo emailInfo) {
        try {
            String url = constructUrl(combine("/internal/emails/s3template/update", tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendS3TemplateUpdateEmail", url, emailInfo);
        } catch (Exception e) {
            throw new RuntimeException("sendS3TemplateUpdateEmail: Remote call failure", e);
        }
    }

    public void sendS3TemplateCreateEmail(String tenantId, S3ImportEmailInfo emailInfo) {
        try {
            String url = constructUrl(combine("/internal/emails/s3template/create", tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendS3TemplateCreateEmail", url, emailInfo);
        } catch (Exception e) {
            throw new RuntimeException("sendS3TemplateCreateEmail: Remote call failure", e);
        }
    }

    @Override
    public ScoringRequestConfigContext retrieveScoringRequestConfigContext(String configUuid) {
        try {
            String url = constructUrl(
                    "/internal/external-scoring-config-context/" + configUuid);
            if (log.isDebugEnabled()) {
                log.debug("Find ScoringRequestConfigContext by configId (" + configUuid + ")" + url);
            }
            return get("retrieveScoringRequestConfigContext", url, ScoringRequestConfigContext.class);
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
        urlStr.append("/internal/jobs/all/").append(CustomerSpace.parse(customerSpace).toString());
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

    @Override
    public void registerReport(Report report, String tenantId) {
        try {
            String url = constructUrl(combine("/internal/reports", tenantId));
            log.info(String.format("Posting to %s\n%s", url, JsonUtils.pprint(report)));
            post("registerReport", url, report, Void.class);
        } catch (Exception e) {
            throw new RuntimeException("registerReport: Remote call failure", e);

        }
    }

    @Override
    public Report findReportByName(String name, String tenantId) {
        try {
            String url = constructUrl(combine("/internal/reports", name, tenantId));
            log.info(String.format("Getting from %s", url));
            return get("findReportByName", url, Report.class);
        } catch (Exception e) {
            throw new RuntimeException("findReportByName: Remote call failure", e);
        }
    }

    @Override
    public void copyNotes(String fromModelSummaryId, String toModelSummaryId) {
        try {
            String url = constructUrl(combine("/internal/modelnotes/", fromModelSummaryId, toModelSummaryId));
            log.debug(String.format("Copy note from ModelSummary %s to ModelSummary %s, url %s", fromModelSummaryId, toModelSummaryId, url));
            post("copyNotes", url, null, Boolean.class);
        } catch (Exception e) {
            throw new RuntimeException("CopyNotes: Remote call failure", e);
        }
    }

    public void deleteTenant(CustomerSpace customerSpace) {
        try {
            String url = constructUrl(combine("/admin/tenants/", customerSpace.toString()));
            log.debug(String.format("Deleting to %s", url));
            delete("deleteTenant", url);
        } catch (Exception e) {
            throw new RuntimeException("deleteTenant: Remote call failure", e);
        }
    }

    @Override
    public void createNote(String modelId, NoteParams noteParams) {
        try {
            String url = constructUrl(combine("/internal/modelnotes/", modelId));
            log.debug(String.format("Creating model %s's note content to %s", modelId, noteParams.getContent(), url));
            post("createNote", url, noteParams, Boolean.class);
        } catch (Exception e) {
            throw new RuntimeException("CreateNote: Remote call failure", e);
        }
    }

    @Override
    public void sendCDLProcessAnalyzeEmail(String result, String tenantId, AdditionalEmailInfo info) {
        try {
            String url = constructUrl(combine("/internal/emails/processanalyze/result", result, tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendCDLProcessAnalyzeEmail", url, info);
        } catch (Exception e) {
            throw new RuntimeException("sendProcessAnalyzeEmail: Remote call failure", e);
        }
    }

    @Override
    public void sendMetadataSegmentExportEmail(String result, String tenantId, MetadataSegmentExport export) {
        try {
            String url = constructUrl(combine("/internal/emails/segmentexport/result", result, tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendMetadataSegmentExportEmail", url, export);
        } catch (Exception e) {
            throw new RuntimeException("sendMetadataSegmentExportEmail: Remote call failure", e);
        }
    }

    @Override
    public void sendAtlasExportEmail(String result, String tenantId, AtlasExport export) {
        try {
            String url = constructUrl(combine("/internal/emails/atlasexport/result", result, tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendMetadataSegmentExportEmail", url, export);
        } catch (Exception e) {
            throw new RuntimeException("sendAtlasExportEmail: Remote call failure", e);
        }
    }

    public void sendOrphanRecordsExportEmail(String result, String tenantId, OrphanRecordsExportRequest export) {
        try {
            String url = constructUrl(combine("/internal/emails/orphanexport/result", result, tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendOrphanRecordsExportEmail", url, export);
        } catch (Exception e) {
            throw new RuntimeException("sendOrphanRecordsExportEmail: Remote call failure", e);
        }
    }

    public boolean createTenant(Tenant tenant) {
        try {
            String url = constructUrl("/admin/tenants");
            log.debug(String.format("Posting to %s", url));
            return post("createTenant", url, tenant, Boolean.class);
        } catch (Exception e) {
            throw new RuntimeException("createTenant: Remote call failure", e);
        }
    }

    @Override
    public void sendPlsCreateModelEmail(String result, String tenantId, AdditionalEmailInfo info) {
        try {
            String url = constructUrl(combine("/internal/emails/createmodel/result", result, tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendPlsCreateModelEmail", url, info);
        } catch (Exception e) {
            throw new RuntimeException("sendCreateModelEmail: Remote call failure", e);
        }
    }

    @Override
    public void sendPlsScoreEmail(String result, String tenantId, AdditionalEmailInfo info) {
        try {
            String url = constructUrl(combine("/internal/emails/score/result", result, tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendPlsScoreEmail", url, info);
        } catch (Exception e) {
            throw new RuntimeException("sendScoreEmail: Remote call failure", e);
        }
    }

    @Override
    public void sendPlsEnrichInternalAttributeEmail(String result, String tenantId,
                                                    AdditionalEmailInfo info) {
        try {
            String url = constructUrl(combine("/internal/emails/enrichment/internal/result", result,
                    tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendPlsEnrichInternalAttributeEmail", url, info);
        } catch (Exception e) {
            throw new RuntimeException("sendScoreEmail: Remote call failure", e);
        }
    }
}
