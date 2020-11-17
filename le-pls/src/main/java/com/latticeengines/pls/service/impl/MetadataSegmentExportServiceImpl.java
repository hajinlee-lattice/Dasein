package com.latticeengines.pls.service.impl;

import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.download.CustomerSpaceHdfsFileDownloader;
import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.cdl.EntityExportRequest;
import com.latticeengines.domain.exposed.cdl.ExportEntity;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.util.MetadataSegmentExportConverter;
import com.latticeengines.pls.service.MetadataSegmentExportService;
import com.latticeengines.pls.util.ExportUtils;
import com.latticeengines.proxy.exposed.cdl.AtlasExportProxy;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;

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
    private AtlasExportProxy atlasExportProxy;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private ImportFromS3Service importFromS3Service;

    @Inject
    private BatonService batonService;

    @Value("${pls.segment.export.max}")
    private Long maxEntryLimitForExport;

    @Override
    public List<MetadataSegmentExport> getSegmentExports() {
        String customerSpace = getCustomerSpace().toString();
        List<AtlasExport> atlasExports = atlasExportProxy.findAll(getCustomerSpace().toString());
        List<MetadataSegmentExport> metadataSegmentExports =
                atlasExports.stream().map(AtlasExport -> MetadataSegmentExportConverter.convertToMetadataSegmentExport(AtlasExport)).collect(Collectors.toList());
        List<MetadataSegmentExport> result = segmentProxy.getMetadataSegmentExports(customerSpace);
        result.addAll(metadataSegmentExports);
        return result;
    }

    @Override
    public MetadataSegmentExport getSegmentExportByExportId(String exportId) {
        MetadataSegmentExport metadataSegmentExport = segmentProxy.getMetadataSegmentExport(getCustomerSpace().toString(), exportId);
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
    public MetadataSegmentExport createSegmentExportJob(MetadataSegmentExport metadataSegmentExportJob) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        String customerSpaceStr = customerSpace.toString();
        DataCollection.Version version = dataCollectionProxy.getActiveVersion(customerSpaceStr);
        setCreatedBy(metadataSegmentExportJob);
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
        segmentProxy.deleteMetadataSegmentExport(MultiTenantContext.getTenant().getId(), exportId);
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
        String customerSpace = getCustomerSpace().toString();
        MetadataSegmentExport metadataSegmentExport = segmentProxy.getMetadataSegmentExport(customerSpace, exportId);
        if (metadataSegmentExport == null) {
            // handle new download
            downloadAtlasExport(customerSpace, exportId, request, response);
        } else {
            // handle old download
            downloadSegmentExport(metadataSegmentExport, exportId, request, response);
        }
    }

    private String getFilePath(String filePath, String fileName) {
        StringBuffer result = new StringBuffer();
        return result.append(filePath).append(fileName).toString();
    }

    private void downloadAtlasExport(String customerSpace, String exportId, HttpServletRequest request,
                                     HttpServletResponse response) {
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
                ExportUtils.downloadS3ExportFile(getFilePath(filePath, fileName), fileName, "application/csv", request
                        , response, importFromS3Service, batonService);
            } else if (CollectionUtils.isNotEmpty(atlasExport.getFilesUnderSystemPath())) {
                filePath = atlasExportProxy.getSystemExportPath(customerSpace, false);
                fileName = atlasExport.getFilesUnderSystemPath().get(0);
                // if segment name include special characters, MYSQL @Type(type = "json") annotation can't handle filesToDelete well, so we
                // need to rebuild file name here
                if (StringUtils.isNotEmpty(atlasExport.getSegmentName())) {
                    String suffix = fileName.endsWith(".csv.gz") ? ".csv.gz" : ".csv";
                    String exportType = getExportType(atlasExport);
                    if (StringUtils.isNotEmpty(exportType)) {
                        fileName = exportType + "_" + atlasExport.getSegmentName() + "_" + atlasExport.getUuid() + suffix;
                    }
                }
                ExportUtils.downloadS3ExportFile(getFilePath(filePath, fileName), fileName, "application/csv",
                        request, response, importFromS3Service, batonService);
            } else {
                throw new LedpException(LedpCode.LEDP_18161, new Object[]{exportId});
            }
        } catch (Exception ex) {
            log.error("Could not download result of export job: " + exportId, ex);
            throw new LedpException(LedpCode.LEDP_18161, new Object[]{exportId});
        }
    }

    private String getExportType(AtlasExport atlasExport) {
        if (atlasExport.getExportType() != null) {
            switch (atlasExport.getExportType()) {
                case ACCOUNT:
                    return ExportEntity.Account.name();
                case CONTACT:
                    return ExportEntity.Contact.name();
                case ACCOUNT_AND_CONTACT:
                    return ExportEntity.AccountContact.name();
                default:
                    return null;
            }
        }
        return null;
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

}
