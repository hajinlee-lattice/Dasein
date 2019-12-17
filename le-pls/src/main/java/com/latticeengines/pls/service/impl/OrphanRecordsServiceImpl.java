package com.latticeengines.pls.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsExportRequest;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.service.OrphanRecordsService;
import com.latticeengines.pls.util.ExportUtils;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("orphanRecordsService")
public class OrphanRecordsServiceImpl implements OrphanRecordsService {

    private static final Logger log = LoggerFactory.getLogger(OrphanRecordsServiceImpl.class);

    @Inject
    private ImportFromS3Service importFromS3Service;

    @Inject
    private BatonService batonService;

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private WorkflowProxy workflowProxy;

    @Override
    public Job submitOrphanRecordsWorkflow(String orphanType, HttpServletResponse response) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        log.info(String.format("Received call to launch orphan records workflow for tenant=%s", customerSpace));
        OrphanRecordsExportRequest request = new OrphanRecordsExportRequest();
        request.setOrphanRecordsType(OrphanRecordsType.valueOf(orphanType.toUpperCase()));
        request.setOrphanRecordsArtifactStatus(DataCollectionArtifact.Status.NOT_SET);
        request.setCreatedBy(MultiTenantContext.getEmailAddress());
        request.setExportId(UUID.randomUUID().toString());
        request.setArtifactVersion(null);
        log.info(String.format("Start to submit orphan records workflow. Tenant=%s. Request=%s",
                customerSpace, JsonUtils.serialize(request)));
        ApplicationId applicationId = cdlProxy.submitOrphanRecordsExport(customerSpace, request);
        if (applicationId == null) {
            try {
                Job failedJob = new Job();
                failedJob.setApplicationId(null);
                failedJob.setErrorMsg("Required tables are not uploaded.");
                failedJob.setJobStatus(JobStatus.FAILED);
                return failedJob;
            } catch (Exception exc) {
                log.warn(String.format("Getting exception while returning failed job. Exception=%s.",
                        exc.getMessage()));
                throw new RuntimeException(exc);
            }
        }
        try {
            return workflowProxy.getWorkflowJobFromApplicationId(applicationId.toString(), customerSpace);
        } catch (Exception exc) {
            log.warn(String.format("Getting exception while waiting for job=%s. Exception=%s.", applicationId.toString(), exc.getMessage()));
            throw new RuntimeException(exc);
        }
    }

    @Override
    public void downloadOrphanArtifact(String exportId, HttpServletRequest request, HttpServletResponse response) {
        log.info("Received call to download orphan artifact. ExportId=" + exportId);
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        String filePath = dataCollectionProxy.getDataCollectionArtifactPath(customerSpace, exportId);
        if (StringUtils.isEmpty(filePath)) {
            throw new LedpException(LedpCode.LEDP_18161, new Object[]{exportId});
        }
        String filename = exportId + ".csv";
        ExportUtils.downloadS3ExportFile(filePath, filename, MediaType.APPLICATION_OCTET_STREAM, request, response, importFromS3Service, batonService);
    }

    @Override
    public String getOrphanRecordsCount(String orphanType) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        DataCollection.Version active = dataCollectionProxy.getActiveVersion(customerSpace);
        log.info(String.format("Get orphan records count for tenant=%s, version=%s", customerSpace, active));
        DataCollectionStatus status = dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace, active);
        Map<String, Long> result = new HashMap<>();
        if (StringUtils.isNotBlank(orphanType)) {
            switch (OrphanRecordsType.valueOf(orphanType.toUpperCase())) {
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
