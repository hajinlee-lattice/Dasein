package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableList;
import com.latticeengines.app.exposed.download.CustomerSpaceHdfsFileDownloader;
import com.latticeengines.app.exposed.download.CustomerSpaceS3FileDownloader;
import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.cdl.EntityExportRequest;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.util.MetadataSegmentExportConverter;
import com.latticeengines.domain.exposed.util.SegmentExportUtil;
import com.latticeengines.pls.entitymanager.MetadataSegmentExportEntityMgr;
import com.latticeengines.pls.service.MetadataSegmentExportService;
import com.latticeengines.pls.workflow.SegmentExportWorkflowSubmitter;
import com.latticeengines.proxy.exposed.cdl.AtlasExportProxy;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

@Component("metadataSegmentExportService")
public class MetadataSegmentExportServiceImpl implements MetadataSegmentExportService {

    private static final Logger log = LoggerFactory.getLogger(MetadataSegmentExportServiceImpl.class);

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private MetadataSegmentExportEntityMgr metadataSegmentExportEntityMgr;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private AtlasExportProxy atlasExportProxy;

    @Inject
    private SegmentExportWorkflowSubmitter segmentExportWorkflowSubmitter;

    @Inject
    private ImportFromS3Service importFromS3Service;

    @Inject
    private BatonService batonService;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Value("${pls.segment.export.max}")
    private Long maxEntryLimitForExport;

   @Override
    public List<MetadataSegmentExport> getSegmentExports() {
        List<AtlasExport> atlasExports = atlasExportProxy.findAll(getCustomerSpace().toString());
        List<MetadataSegmentExport> metadataSegmentExports =
                atlasExports.stream().map(AtlasExport -> MetadataSegmentExportConverter.convertToMetadataSegmentExport(AtlasExport)).collect(Collectors.toList());
        List<MetadataSegmentExport> result = metadataSegmentExportEntityMgr.findAll();
        result.addAll(metadataSegmentExports);
        return result;
    }

    @Override
    public MetadataSegmentExport getSegmentExportByExportId(String exportId) {
        MetadataSegmentExport metadataSegmentExport = metadataSegmentExportEntityMgr.findByExportId(exportId);
        if (metadataSegmentExport != null) {
            return metadataSegmentExport;
        }
        AtlasExport atlasExport = atlasExportProxy.findAtlasExportById(getCustomerSpace().toString(), exportId);
        return MetadataSegmentExportConverter.convertToMetadataSegmentExport(atlasExport);
    }

    private CustomerSpace getCustomerSpace() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return customerSpace;
    }

    @Override
    public MetadataSegmentExport createSegmentExportJob(MetadataSegmentExport metadataSegmentExportJob, Boolean useSparkFromRestApi) {
        boolean useSpark;
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (useSparkFromRestApi != null) {
            useSpark = useSparkFromRestApi;
        } else {
            useSpark = batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_EXPORT_WITH_SPARK_SQL);
        }
        String customerSpaceStr = customerSpace.toString();
        DataCollection.Version version = dataCollectionProxy.getActiveVersion(customerSpaceStr);
        checkExportAttribute(metadataSegmentExportJob, customerSpaceStr, version);
        if (!useSpark) {
            // check size limitation for old segment export logic
            checkExportSize(metadataSegmentExportJob);
        }
        setCreatedBy(metadataSegmentExportJob);
        if (useSpark) {
            if (customerSpace == null) {
                throw new LedpException(LedpCode.LEDP_18217);
            }
            EntityExportRequest request = new EntityExportRequest();
            request.setDataCollectionVersion(version);
            AtlasExport atlasExport = atlasExportProxy.createAtlasExport(customerSpaceStr, MetadataSegmentExportConverter.convertToAtlasExport(metadataSegmentExportJob));
            request.setAtlasExportId(atlasExport.getUuid());
            request.setSaveToDropfolder(false);
            cdlProxy.entityExport(customerSpaceStr, request);
            return MetadataSegmentExportConverter.convertToMetadataSegmentExport(atlasExportProxy.findAtlasExportById(customerSpaceStr, atlasExport.getUuid()));
        } else {
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
    }

    private void checkExportAttribute(MetadataSegmentExport metadataSegmentExport, String customerSpace,
                                      DataCollection.Version version) {
        List<ColumnSelection.Predefined> groups = Collections.singletonList(ColumnSelection.Predefined.Enrichment);
        List<ColumnMetadata> cms = new ArrayList<>();
        if (AtlasExportType.ACCOUNT_AND_CONTACT.equals(metadataSegmentExport.getType())) {
            cms = servingStoreProxy.getDecoratedMetadata(customerSpace, ImmutableList.copyOf(BusinessEntity.EXPORT_ENTITIES), groups, version);
        } else if (AtlasExportType.ACCOUNT.equals(metadataSegmentExport.getType())) {
            List<BusinessEntity> businessEntities = BusinessEntity.EXPORT_ENTITIES.stream().collect(Collectors.toList());
            businessEntities.remove(BusinessEntity.Contact);
            cms = servingStoreProxy.getDecoratedMetadata(customerSpace, businessEntities, groups, version);
        } else if (AtlasExportType.CONTACT.equals(metadataSegmentExport.getType())) {
            List<BusinessEntity> businessEntities = new ArrayList<>();
            businessEntities.add(BusinessEntity.Contact);
            cms = servingStoreProxy.getDecoratedMetadata(customerSpace, businessEntities, groups, version);
        }
        log.info("Total attributes for export = " + cms.size());
        if (cms.size() == 0) {
            throw new LedpException(LedpCode.LEDP_18231, new String[]{metadataSegmentExport.getType().name()});
        }
    }

    public static void main(String[] args) {
        List<BusinessEntity> businessEntities = BusinessEntity.EXPORT_ENTITIES.stream().collect(Collectors.toList());
        businessEntities.remove(BusinessEntity.Contact);
    }

    private void checkExportSize(MetadataSegmentExport metadataSegmentExportJob) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setAccountRestriction(metadataSegmentExportJob.getAccountFrontEndRestriction());
        frontEndQuery.setContactRestriction(metadataSegmentExportJob.getContactFrontEndRestriction());
        frontEndQuery.setMainEntity(metadataSegmentExportJob.getType() == AtlasExportType.ACCOUNT
                ? BusinessEntity.Account : BusinessEntity.Contact);
        Tenant tenant = MultiTenantContext.getTenant();
        Long entriesCount = entityProxy.getCount(tenant.getId(), frontEndQuery);
        log.info("Total entries for export = " + entriesCount);
        if (entriesCount > maxEntryLimitForExport) {
            throw new LedpException(LedpCode.LEDP_18169, new String[]{maxEntryLimitForExport.toString()});
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

    private void throwDownloadExportException(MetadataSegmentExport.Status status, String exportId) {
        if (!MetadataSegmentExport.Status.COMPLETED.equals(status)) {
            switch (status) {
                case RUNNING:
                    throw new LedpException(LedpCode.LEDP_18163, new Object[]{exportId});
                case FAILED:
                    throw new LedpException(LedpCode.LEDP_18164, new Object[]{exportId});
                default:
                    throw new LedpException(LedpCode.LEDP_18160, new Object[]{exportId});
            }
        }
    }

    @Override
    public void downloadSegmentExportResult(String exportId, HttpServletRequest request, HttpServletResponse response) {
        MetadataSegmentExport metadataSegmentExport = metadataSegmentExportEntityMgr.findByExportId(exportId);
        if (metadataSegmentExport == null) {
            // handle new download
            downloadAtlasExport(exportId, request, response);
        } else {
            // handle old download
            downloadSegmentExport(metadataSegmentExport, exportId, request, response);
        }
    }

    private void downloadS3ExportFile(String filePath, String fileName, HttpServletRequest request, HttpServletResponse response) {
        String fileNameIndownloader = fileName;
        if (fileNameIndownloader.endsWith(".gz")){
            fileNameIndownloader = fileNameIndownloader.substring(0, fileNameIndownloader.length() - 3);
        }
        response.setHeader("Content-Encoding", "gzip");
        CustomerSpaceS3FileDownloader.S3FileDownloadBuilder builder = new CustomerSpaceS3FileDownloader.S3FileDownloadBuilder();
        builder.setMimeType("application/csv").setFilePath(filePath + fileName).setFileName(fileNameIndownloader).setImportFromS3Service(importFromS3Service).setBatonService(batonService);
        CustomerSpaceS3FileDownloader customerSpaceS3FileDownloader = new CustomerSpaceS3FileDownloader(builder);
        customerSpaceS3FileDownloader.downloadFile(request, response);
    }

    private void downloadAtlasExport(String exportId, HttpServletRequest request, HttpServletResponse response) {
        String customerSpace = getCustomerSpace().toString();
        AtlasExport atlasExport = atlasExportProxy.findAtlasExportById(customerSpace, exportId);
        if (atlasExport == null || atlasExport.getCleanupBy().getTime() < System.currentTimeMillis()) {
            throw new LedpException(LedpCode.LEDP_18160, new Object[]{exportId});
        }
        if (!MetadataSegmentExport.Status.COMPLETED.equals(atlasExport.getStatus())) {
            throwDownloadExportException(atlasExport.getStatus(), exportId);
        }
        try {
            String filePath;
            String fileName;
            if (CollectionUtils.isNotEmpty(atlasExport.getFilesUnderDropFolder())) {
                filePath = atlasExportProxy.getDropFolderExportPath(customerSpace, atlasExport.getExportType(), atlasExport.getDatePrefix(), false);
                fileName = atlasExport.getFilesUnderDropFolder().get(0);
                downloadS3ExportFile(filePath, fileName, request, response);
            } else if (CollectionUtils.isNotEmpty(atlasExport.getFilesUnderSystemPath())) {
                filePath = atlasExportProxy.getSystemExportPath(customerSpace, false);
                fileName = atlasExport.getFilesUnderSystemPath().get(0);
                downloadS3ExportFile(filePath, fileName, request, response);
            } else {
                throw new LedpException(LedpCode.LEDP_18161, new Object[]{exportId});
            }
        } catch (Exception ex) {
            log.error("Could not download result of export job: " + exportId, ex);
            throw new LedpException(LedpCode.LEDP_18161, new Object[]{exportId});
        }
    }

    private void downloadSegmentExport(MetadataSegmentExport metadataSegmentExport, String exportId,
                                       HttpServletRequest request, HttpServletResponse response) {
        if (metadataSegmentExport.getCleanupBy().getTime() < System.currentTimeMillis()) {
            throw new LedpException(LedpCode.LEDP_18160, new Object[]{exportId});
        }
        if (!MetadataSegmentExport.Status.COMPLETED.equals(metadataSegmentExport.getStatus())) {
            throwDownloadExportException(metadataSegmentExport.getStatus(), exportId);
        }
        try {
            String filePath = getExportedFilePath(metadataSegmentExport);
            response.setHeader("Content-Encoding", "gzip");
            response.setHeader("Content-Disposition",
                    String.format("attachment; filename=\"%s\"", "segment_export" + ".csv"));
            CustomerSpaceHdfsFileDownloader downloader = getCustomerSpaceDownloader(
                    MediaType.APPLICATION_OCTET_STREAM, filePath, metadataSegmentExport.getFileName());
            downloader.setShouldReformatDate(true);
            downloader.downloadFile(request, response);
        } catch (Exception ex) {
            log.error("Could not download result of export job: " + exportId, ex);
            throw new LedpException(LedpCode.LEDP_18161, new Object[]{exportId});
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
        String customer = MultiTenantContext.getTenant().getId();
        customer = customer != null ? customer : new HdfsToS3PathBuilder().getCustomerFromHdfsPath(filePath);
        builder.setMimeType(mimeType).setFilePath(filePath).setYarnConfiguration(yarnConfiguration)
                .setFileName(fileName).setCustomer(customer).setImportFromS3Service(importFromS3Service)
                .setBatonService(batonService);
        return new CustomerSpaceHdfsFileDownloader(builder);
    }

    private MetadataSegmentExport submitExportWorkflowJob(MetadataSegmentExport metadataSegmentExportJob) {
        ApplicationId applicationId = segmentExportWorkflowSubmitter.submit(metadataSegmentExportJob);
        metadataSegmentExportJob.setApplicationId(applicationId.toString());
        return updateSegmentExportJob(metadataSegmentExportJob);
    }

    @Override
    public MetadataSegmentExport createOrphanRecordThruMgr(MetadataSegmentExport metadataSegmentExport) {
        metadataSegmentExportEntityMgr.create(metadataSegmentExport);
        return metadataSegmentExportEntityMgr.findByExportId(metadataSegmentExport.getExportId());
    }

}
