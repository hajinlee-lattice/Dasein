package com.latticeengines.pls.controller.datacollection;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.codehaus.jackson.map.ObjectMapper;
import org.mortbay.log.Log;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.GzipUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsExportRequest;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsType;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

import io.swagger.annotations.Api;

@Api(value = "data-collection-orphans", description = "REST resource for orphan records")
@RestController
@RequestMapping("/datacollection/orphans")
public class OrphanRecordsResource {

    @Inject
    CDLProxy cdlProxy;

    @Inject
    DataCollectionProxy dataCollectionProxy;

    @Inject
    WorkflowProxy workflowProxy;

    @PostMapping(value = "type/{orphanType}/submit")
    public void submitOrphanRecordsWorkflow(@PathVariable String orphanType, HttpServletResponse response) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        Log.info("Received call to launch orphan records workflow for tenant=" + customerSpace);
        OrphanRecordsExportRequest request = new OrphanRecordsExportRequest();
        request.setOrphanRecordsType(OrphanRecordsType.valueOf(orphanType));
        request.setOrphanRecordsArtifactStatus(DataCollectionArtifact.Status.NOT_SET);
        request.setCreatedBy(MultiTenantContext.getEmailAddress());
        request.setExportId(UUID.randomUUID().toString());
        request.setArtifactVersion(null);
        Log.info(String.format("Start to submit orphan records workflow. Tenant=%s. Request=%s",
                customerSpace, JsonUtils.serialize(request)));
        ApplicationId applicationId = cdlProxy.submitOrphanRecordsExport(customerSpace, request);

        final long maxWaitTime = TimeUnit.MILLISECONDS.convert(24L, TimeUnit.HOURS);
        final long checkInterval = TimeUnit.MILLISECONDS.convert(10L, TimeUnit.SECONDS);
        int retryOnException = 16;
        long start = System.currentTimeMillis();
        response.setHeader("Content-Type", "text/event-stream");
        response.setHeader("Cache-Control", "no-cache");
        do {
            try {
                Job job = workflowProxy.getWorkflowJobFromApplicationId(applicationId.toString(), customerSpace);
                FileCopyUtils.copy(
                        new ByteArrayInputStream(JsonUtils.serialize(job).getBytes()),
                        response.getOutputStream());
                response.getOutputStream().flush();
                if (job.getJobStatus().isTerminated()) {
                    return;
                }
                Thread.sleep(checkInterval);
            } catch (Exception exc) {
                Log.warn(String.format("Getting exception while waiting for job=%s. Retries left=%s. Exception=%s.",
                        applicationId.toString(), retryOnException, exc.getMessage()));
                if (--retryOnException == 0) {
                    throw new RuntimeException(exc);
                }
            }
        } while (System.currentTimeMillis() - start < maxWaitTime);
    }

    @GetMapping(value = "/export/{exportId}/download", headers = "Accept=application/json")
    public void downloadOrphanArtifact(@PathVariable String exportId, HttpServletResponse response) {
        Log.info("Received call to download orphan artifact. ExportId=" + exportId);
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        byte[] file = dataCollectionProxy.downloadDataCollectionArtifact(customerSpace, exportId);
        String filename = exportId + ".csv";
        response.setHeader("Content-Encoding", "gzip");
        response.setHeader("Content-Disposition", String.format("attachment; filename=\"%s\"", filename));
        response.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        try (OutputStream os = response.getOutputStream()) {
            GzipUtils.copyAndCompressStream(new ByteArrayInputStream(file), os);
        } catch (IOException exc) {
            throw new RuntimeException(String.format(
                    "Failed to download artifact from S3. Tenant=%s. ExportId=%s.", customerSpace, exportId));
        }
    }

    @GetMapping(value = "/count")
    public String getOrphanRecordsCount(@RequestParam(required = false) String orphanType) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        DataCollection.Version active = dataCollectionProxy.getActiveVersion(customerSpace);
        Log.info(String.format("Get orphan records count for tenant=%s, version=%s", customerSpace, active));
        DataCollectionStatus status = dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace, active);
        Map<String, Long> result = new HashMap<>();
        if (StringUtils.isNotBlank(orphanType)) {
            switch (OrphanRecordsType.valueOf(orphanType)) {
                case CONTACT:
                    result.put(OrphanRecordsType.CONTACT.getDisplayName(), status.getOrphanContactCount());
                    break;
                case TRANSACTION:
                    result.put(OrphanRecordsType.TRANSACTION.getDisplayName(), status.getOrphanTransactionCount());
                    break;
                case UNMATCHED_ACCOUNT:
                    result.put(OrphanRecordsType.UNMATCHED_ACCOUNT.getDisplayName(), status.getUnmatchedAccountCount());
                    break;
                default:
                    result.put(OrphanRecordsType.CONTACT.getDisplayName(), -1L);
                    result.put(OrphanRecordsType.TRANSACTION.getDisplayName(), -1L);
                    result.put(OrphanRecordsType.UNMATCHED_ACCOUNT.getDisplayName(), -1L);
                    break;
            }
        } else {
            result.put(OrphanRecordsType.CONTACT.getDisplayName(), status.getOrphanContactCount());
            result.put(OrphanRecordsType.TRANSACTION.getDisplayName(), status.getOrphanTransactionCount());
            result.put(OrphanRecordsType.UNMATCHED_ACCOUNT.getDisplayName(), status.getUnmatchedAccountCount());
        }

        return JsonUtils.serialize(result);
    }
}
