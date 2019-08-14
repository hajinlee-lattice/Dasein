package com.latticeengines.workflow.exposed.build;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsExportRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.pls.AdditionalEmailInfo;
import com.latticeengines.domain.exposed.pls.NoteParams;
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

    private String internalResourceHostPort;

    public InternalResourceRestApiProxy(String internalResourceHostPort) {
        super();
        this.internalResourceHostPort = internalResourceHostPort;
    }

    @Override
    public String getRestApiHostPort() {
        return internalResourceHostPort;
    }

    public void registerReport(Report report, String tenantId) {
        try {
            String url = constructUrl("pls/internal/reports", tenantId);
            log.info(String.format("Posting to %s\n%s", url, JsonUtils.pprint(report)));
            restTemplate.postForObject(url, report, Void.class);
        } catch (Exception e) {
            throw new RuntimeException("registerReport: Remote call failure", e);
        }
    }

    public Report findReportByName(String name, String tenantId) {
        try {
            String url = constructUrl("pls/internal/reports", name, tenantId);
            log.info(String.format("Getting from %s", url));
            return restTemplate.getForObject(url, Report.class);
        } catch (Exception e) {
            throw new RuntimeException("findReportByName: Remote call failure", e);
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

    public void sendPlsCreateModelEmail(String result, String tenantId, AdditionalEmailInfo info) {
        try {
            String url = constructUrl("pls/internal/emails/createmodel/result", result, tenantId);
            log.info(String.format("Putting to %s", url));
            restTemplate.put(url, info);
        } catch (Exception e) {
            throw new RuntimeException("sendCreateModelEmail: Remote call failure", e);
        }
    }

    public void sendPlsScoreEmail(String result, String tenantId, AdditionalEmailInfo info) {
        try {
            String url = constructUrl("pls/internal/emails/score/result", result, tenantId);
            log.info(String.format("Putting to %s", url));
            restTemplate.put(url, info);
        } catch (Exception e) {
            throw new RuntimeException("sendScoreEmail: Remote call failure", e);
        }
    }

    public void sendPlsEnrichInternalAttributeEmail(String result, String tenantId,
                                                    AdditionalEmailInfo info) {
        try {
            String url = constructUrl("pls/internal/emails/enrichment/internal/result", result,
                    tenantId);
            log.info(String.format("Putting to %s", url));
            restTemplate.put(url, info);
        } catch (Exception e) {
            throw new RuntimeException("sendScoreEmail: Remote call failure", e);
        }
    }

    public void sendCDLProcessAnalyzeEmail(String result, String tenantId, AdditionalEmailInfo info) {
        try {
            String url = constructUrl("pls/internal/emails/processanalyze/result", result, tenantId);
            log.info(String.format("Putting to %s", url));
            restTemplate.put(url, info);
        } catch (Exception e) {
            throw new RuntimeException("sendProcessAnalyzeEmail: Remote call failure", e);
        }
    }

    public void sendOrphanRecordsExportEmail(String result, String tenantId, OrphanRecordsExportRequest export) {
        try {
            String url = constructUrl("pls/internal/emails/orphanexport/result", result, tenantId);
            log.info(String.format("Putting to %s", url));
            restTemplate.put(url, export);
        } catch (Exception e) {
            throw new RuntimeException("sendOrphanRecordsExportEmail: Remote call failure", e);
        }
    }

    public void sendS3ImportEmail(String result, String tenantId, S3ImportEmailInfo emailInfo) {
        try {
            String url = constructUrl("pls/internal/emails/s3import/result", result, tenantId);
            log.info(String.format("Putting to %s", url));
            restTemplate.put(url, emailInfo);
        } catch (Exception e) {
            throw new RuntimeException("sendS3ImportEmail: Remote call failure", e);
        }
    }

    public void createNote(String modelId, NoteParams noteParams) {
        try {
            String url = constructUrl("pls/internal/modelnotes/", modelId);
            log.debug(String.format("Creating model %s's note content to %s", modelId, noteParams.getContent(), url));
            restTemplate.postForEntity(url, noteParams, Boolean.class);
        } catch (Exception e) {
            throw new RuntimeException("CreateNote: Remote call failure", e);
        }
    }

    public void copyNotes(String fromModelSummaryId, String toModelSummaryId) {
        try {
            String url = constructUrl("pls/internal/modelnotes/", fromModelSummaryId, toModelSummaryId);
            HttpHeaders headers = new HttpHeaders();
            HttpEntity<Void> request = new HttpEntity<>(headers);
            log.debug(String.format("Copy note from ModelSummary %s to ModelSummary %s, url %s", fromModelSummaryId, toModelSummaryId, url));
            restTemplate.exchange(url, HttpMethod.POST, request, Boolean.class);
        } catch (Exception e) {
            throw new RuntimeException("CopyNotes: Remote call failure", e);
        }
    }
}
