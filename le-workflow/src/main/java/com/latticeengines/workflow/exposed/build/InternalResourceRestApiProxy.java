package com.latticeengines.workflow.exposed.build;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsExportRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.pls.AdditionalEmailInfo;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

/*
 * this suppress warning is for DeprecatedBaseRestApiProxy class
 * the plan is to remove all the proxy that uses DeprecatedBaseRestApiProxy in the future,
 * so suppress for now.
 */
@SuppressWarnings("deprecation")
public class InternalResourceRestApiProxy extends BaseRestApiProxy {

    private static final Logger log = LoggerFactory.getLogger(InternalResourceRestApiProxy.class);

    public InternalResourceRestApiProxy(String internalResourceHostPort) {
        super(internalResourceHostPort, "pls");
    }

    public void registerReport(Report report, String tenantId) {
        try {
            String url = constructUrl(combine("/internal/reports", tenantId));
            log.info(String.format("Posting to %s\n%s", url, JsonUtils.pprint(report)));
            post("registerReport", url, report, Void.class);
        } catch (Exception e) {
            throw new RuntimeException("registerReport: Remote call failure", e);
        }
    }

    public Report findReportByName(String name, String tenantId) {
        try {
            String url = constructUrl(combine("/internal/reports", name, tenantId));
            log.info(String.format("Getting from %s", url));
            return get("findReportByName", url, Report.class);
        } catch (Exception e) {
            throw new RuntimeException("findReportByName: Remote call failure", e);
        }
    }

    public SourceFile findSourceFileByName(String name, String tenantId) {
        try {
            String url = constructUrl(combine("/internal/sourcefiles", name, tenantId));
            log.info(String.format("Getting from %s", url));
            return get("findSourceFileByName", url, SourceFile.class);
        } catch (Exception e) {
            throw new RuntimeException("findSourceFileByName: Remote call failure", e);
        }
    }

    public void updateSourceFile(SourceFile sourceFile, String tenantId) {
        try {
            String url = constructUrl(combine("/internal/sourcefiles", sourceFile.getName(), tenantId));
            log.info(String.format("Putting to %s", url));
            put("updateSourceFile", url, sourceFile);
        } catch (Exception e) {
            throw new RuntimeException("updateSourceFile: Remote call failure", e);
        }
    }

    public void createSourceFile(SourceFile sourceFile, String tenantId) {
        try {
            String url = constructUrl(combine("/internal/sourcefiles", sourceFile.getName(), tenantId));
            log.info(String.format("Posting to %s", url));
            post("createSourceFile", url, sourceFile, Void.class);
        } catch (Exception e) {
            throw new RuntimeException("createSourceFile: Remote call failure", e);
        }
    }

    public void sendPlsCreateModelEmail(String result, String tenantId, AdditionalEmailInfo info) {
        try {
            String url = constructUrl(combine("/internal/emails/createmodel/result", result, tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendPlsCreateModelEmail", url, info);
        } catch (Exception e) {
            throw new RuntimeException("sendCreateModelEmail: Remote call failure", e);
        }
    }

    public void sendPlsScoreEmail(String result, String tenantId, AdditionalEmailInfo info) {
        try {
            String url = constructUrl(combine("/internal/emails/score/result", result, tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendPlsScoreEmail", url, info);
        } catch (Exception e) {
            throw new RuntimeException("sendScoreEmail: Remote call failure", e);
        }
    }

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

    public void sendCDLProcessAnalyzeEmail(String result, String tenantId, AdditionalEmailInfo info) {
        try {
            String url = constructUrl(combine("/internal/emails/processanalyze/result", result, tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendCDLProcessAnalyzeEmail", url, info);
        } catch (Exception e) {
            throw new RuntimeException("sendProcessAnalyzeEmail: Remote call failure", e);
        }
    }

    public void sendMetadataSegmentExportEmail(String result, String tenantId, MetadataSegmentExport export) {
        try {
            String url = constructUrl(combine("/internal/emails/segmentexport/result", result, tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendMetadataSegmentExportEmail", url, export);
        } catch (Exception e) {
            throw new RuntimeException("sendMetadataSegmentExportEmail: Remote call failure", e);
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

    public void sendS3ImportEmail(String result, String tenantId, S3ImportEmailInfo emailInfo) {
        try {
            String url = constructUrl(combine("/internal/emails/s3import/result", result, tenantId));
            log.info(String.format("Putting to %s", url));
            put("sendS3ImportEmail", url, emailInfo);
        } catch (Exception e) {
            throw new RuntimeException("sendS3ImportEmail: Remote call failure", e);
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

    public void deleteTenant(CustomerSpace customerSpace) {
        try {
            String url = constructUrl(combine("/admin/tenants/", customerSpace.toString()));
            log.debug(String.format("Deleting to %s", url));
            delete("deleteTenant", url);
        } catch (Exception e) {
            throw new RuntimeException("deleteTenant: Remote call failure", e);
        }
    }

    public void createNote(String modelId, NoteParams noteParams) {
        try {
            String url = constructUrl(combine("/internal/modelnotes/", modelId));
            log.debug(String.format("Creating model %s's note content to %s", modelId, noteParams.getContent(), url));
            post("createNote", url, noteParams, Boolean.class);
        } catch (Exception e) {
            throw new RuntimeException("CreateNote: Remote call failure", e);
        }
    }

    public void copyNotes(String fromModelSummaryId, String toModelSummaryId) {
        try {
            String url = constructUrl(combine("/internal/modelnotes/", fromModelSummaryId, toModelSummaryId));
            log.debug(String.format("Copy note from ModelSummary %s to ModelSummary %s, url %s", fromModelSummaryId, toModelSummaryId, url));
            post("copyNotes", url, null, Boolean.class);
        } catch (Exception e) {
            throw new RuntimeException("CopyNotes: Remote call failure", e);
        }
    }
}
