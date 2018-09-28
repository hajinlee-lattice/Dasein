package com.latticeengines.pls.service.impl;

import java.util.List;
import java.util.UUID;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.download.CustomerSpaceHdfsFileDownloader;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExportType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.SegmentExportUtil;
import com.latticeengines.pls.entitymanager.MetadataSegmentExportEntityMgr;
import com.latticeengines.pls.service.MetadataSegmentExportService;
import com.latticeengines.pls.workflow.SegmentExportWorkflowSubmitter;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

@Component("metadataSegmentExportService")
public class MetadataSegmentExportServiceImpl implements MetadataSegmentExportService {

    private static final Logger log = LoggerFactory.getLogger(MetadataSegmentExportServiceImpl.class);

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private MetadataSegmentExportEntityMgr metadataSegmentExportEntityMgr;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private SegmentExportWorkflowSubmitter segmentExportWorkflowSubmitter;

    @Value("${pls.segment.export.max}")
    private Long maxEntryLimitForExport;

    @Override
    public List<MetadataSegmentExport> getSegmentExports() {
        return metadataSegmentExportEntityMgr.findAll();
    }

    @Override
    public MetadataSegmentExport getSegmentExportByExportId(String exportId) {
        return metadataSegmentExportEntityMgr.findByExportId(exportId);
    }

    @Override
    public MetadataSegmentExport createSegmentExportJob(MetadataSegmentExport metadataSegmentExportJob) {
        checkExportSize(metadataSegmentExportJob);

        setCreatedBy(metadataSegmentExportJob);

        String displayName = "";
        if (metadataSegmentExportJob.getSegment() != null) {
            displayName = metadataSegmentExportJob.getSegment().getDisplayName();
        }
        String exportedFileName = SegmentExportUtil.constructFileName(metadataSegmentExportJob.getExportPrefix(),
                displayName, metadataSegmentExportJob.getType());
        String tableName = "segment_export_" + UUID.randomUUID().toString().replaceAll("-", "_");

        metadataSegmentExportJob.setFileName(exportedFileName);
        metadataSegmentExportJob.setTableName(tableName);

        metadataSegmentExportEntityMgr.create(metadataSegmentExportJob);

        submitExportWorkflowJob(metadataSegmentExportJob);

        return metadataSegmentExportEntityMgr.findByExportId(metadataSegmentExportJob.getExportId());
    }

    private void checkExportSize(MetadataSegmentExport metadataSegmentExportJob) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setAccountRestriction(metadataSegmentExportJob.getAccountFrontEndRestriction());
        frontEndQuery.setContactRestriction(metadataSegmentExportJob.getContactFrontEndRestriction());
        frontEndQuery.setMainEntity(metadataSegmentExportJob.getType() == MetadataSegmentExportType.ACCOUNT
                ? BusinessEntity.Account : BusinessEntity.Contact);

        Tenant tenant = MultiTenantContext.getTenant();

        Long entriesCount = entityProxy.getCount(tenant.getId(), frontEndQuery);
        log.info("Total entries for export = " + entriesCount);
        if (entriesCount > maxEntryLimitForExport) {
            throw new LedpException(LedpCode.LEDP_18169, new String[] { maxEntryLimitForExport.toString() });
        }
    }

    @Override
    public MetadataSegmentExport updateSegmentExportJob(MetadataSegmentExport metadataSegmentExportJob) {
        metadataSegmentExportEntityMgr.createOrUpdate(metadataSegmentExportJob);
        return metadataSegmentExportEntityMgr.findByExportId(metadataSegmentExportJob.getExportId());
    }

    private void setCreatedBy(MetadataSegmentExport metadataSegmentExportJob) {
        Object principal = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        if (principal != null) {
            String email = principal.toString();
            if (StringUtils.isNotBlank(email)) {
                metadataSegmentExportJob.setCreatedBy(email);
            }
        }
        if (metadataSegmentExportJob.getCreatedBy() == null) {
            metadataSegmentExportJob.setCreatedBy("default@lattice-engines.com");
        }
    }

    @Override
    public void deleteSegmentExportByExportId(String exportId) {
        metadataSegmentExportEntityMgr.deleteByExportId(exportId);
    }

    @Override
    public void downloadSegmentExportResult(String exportId, HttpServletRequest request, HttpServletResponse response) {
        MetadataSegmentExport metadataSegmentExport = metadataSegmentExportEntityMgr.findByExportId(exportId);
        if (metadataSegmentExport == null
                || metadataSegmentExport.getCleanupBy().getTime() < System.currentTimeMillis()) {
            throw new LedpException(LedpCode.LEDP_18160, new Object[] { exportId });
        }

        switch (metadataSegmentExport.getStatus()) {
        case RUNNING:
            throw new LedpException(LedpCode.LEDP_18163, new Object[] { exportId });
        case FAILED:
            throw new LedpException(LedpCode.LEDP_18164, new Object[] { exportId });
        case COMPLETED:
            try {
                String filePath = getExportedFilePath(metadataSegmentExport);
                response.setHeader("Content-Encoding", "gzip");
                CustomerSpaceHdfsFileDownloader downloader = getCustomerSpaceDownloader(
                        MediaType.APPLICATION_OCTET_STREAM, filePath, metadataSegmentExport.getFileName());
                downloader.downloadFile(request, response);
            } catch (Exception ex) {
                log.error("Could not download result of export job: " + exportId, ex);
                throw new LedpException(LedpCode.LEDP_18161, new Object[] { exportId });
            }
            break;
        default:
            throw new LedpException(LedpCode.LEDP_18160, new Object[] { exportId });
        }
    }

    @Override
    public String getExportedFilePath(MetadataSegmentExport metadataSegmentExport) {
        String filePath = metadataSegmentExport.getPath();
        filePath = filePath.substring(0, filePath.length() - 1);
        return filePath + "_" + metadataSegmentExport.getFileName();
    }

    private CustomerSpaceHdfsFileDownloader getCustomerSpaceDownloader(String mimeType, String filePath,
            String fileName) {
        CustomerSpaceHdfsFileDownloader.FileDownloadBuilder builder = new CustomerSpaceHdfsFileDownloader.FileDownloadBuilder();
        builder.setMimeType(mimeType).setFilePath(filePath).setYarnConfiguration(yarnConfiguration)
                .setFileName(fileName);
        return new CustomerSpaceHdfsFileDownloader(builder);
    }

    private MetadataSegmentExport submitExportWorkflowJob(MetadataSegmentExport metadataSegmentExportJob) {
        ApplicationId applicationId = segmentExportWorkflowSubmitter.submit(metadataSegmentExportJob);
        metadataSegmentExportJob.setApplicationId(applicationId.toString());
        return updateSegmentExportJob(metadataSegmentExportJob);
    }

    @Override
    public MetadataSegmentExport createOrphanRecordThruMgr(MetadataSegmentExport metadataSegmentExport){
        metadataSegmentExportEntityMgr.create(metadataSegmentExport);
        return metadataSegmentExportEntityMgr.findByExportId(metadataSegmentExport.getExportId());
    }
}
