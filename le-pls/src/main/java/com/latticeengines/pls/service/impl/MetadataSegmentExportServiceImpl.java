package com.latticeengines.pls.service.impl;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.avro.Schema.Type;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.download.CustomerSpaceHdfsFileDownloader;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport.ExportType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.pls.entitymanager.MetadataSegmentExportEntityMgr;
import com.latticeengines.pls.service.MetadataSegmentExportService;
import com.latticeengines.pls.workflow.SegmentExportWorkflowSubmitter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("metadataSegmentExportService")
public class MetadataSegmentExportServiceImpl implements MetadataSegmentExportService {

    private static final Logger log = LoggerFactory.getLogger(MetadataSegmentExportServiceImpl.class);

    private static final String DEFAULT_EXPORT_FILE_PREFIX = "unknownsegment";
    private static final String ACCOUNT_PREFIX = BusinessEntity.Account + "_";
    private static final String CONTACT_PREFIX = BusinessEntity.Contact + "_";

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private MetadataSegmentExportEntityMgr metadataSegmentExportEntityMgr;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private SegmentExportWorkflowSubmitter segmentExportWorkflowSubmitter;

    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd_HH-mm-ss";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);
    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

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

        setCreatedBy(metadataSegmentExportJob);

        String exportedFileName = createFileName(metadataSegmentExportJob);
        metadataSegmentExportJob.setFileName(exportedFileName);

        metadataSegmentExportJob = registerTableForExport(metadataSegmentExportJob);

        metadataSegmentExportEntityMgr.create(metadataSegmentExportJob);

        submitExportWorkflowJob(metadataSegmentExportJob);

        return metadataSegmentExportEntityMgr.findByExportId(metadataSegmentExportJob.getExportId());
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

    private String createFileName(MetadataSegmentExport metadataSegmentExportJob) {
        String exportedFileName = null;
        if (StringUtils.isNotBlank(metadataSegmentExportJob.getExportPrefix())) {
            exportedFileName = metadataSegmentExportJob.getExportPrefix();
        } else if (metadataSegmentExportJob.getSegment() != null
                && StringUtils.isNotBlank(metadataSegmentExportJob.getSegment().getDisplayName())) {
            exportedFileName = metadataSegmentExportJob.getSegment().getDisplayName();
        }

        if (StringUtils.isBlank(exportedFileName)) {
            exportedFileName = DEFAULT_EXPORT_FILE_PREFIX;
        }
        exportedFileName = exportedFileName.trim().replaceAll("[^a-zA-Z0-9]", "");

        exportedFileName += "-" + metadataSegmentExportJob.getType() + "-" + dateFormat.format(new Date()) + "_UTC.csv";
        return exportedFileName;
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

    private MetadataSegmentExport registerTableForExport(MetadataSegmentExport metadataSegmentExportJob) {
        CustomerSpace customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId());

        Table segmentExportTable = new Table();
        ExportType exportType = metadataSegmentExportJob.getType();

        String[] fields = null;
        String[] fieldDisplayNames = null;
        if (exportType == ExportType.ACCOUNT) {
            fields = new String[] { //
                    ACCOUNT_PREFIX + InterfaceName.AccountId.name(), //
                    ACCOUNT_PREFIX + InterfaceName.LDC_Name.name(), //
                    ACCOUNT_PREFIX + InterfaceName.Website.name(), //
                    ACCOUNT_PREFIX + InterfaceName.Address_Street_1.name().toLowerCase(), //
                    ACCOUNT_PREFIX + InterfaceName.City.name(), //
                    ACCOUNT_PREFIX + InterfaceName.State.name(), //
                    ACCOUNT_PREFIX + InterfaceName.PostalCode.name(), //
                    ACCOUNT_PREFIX + InterfaceName.Country.name(), //
                    ACCOUNT_PREFIX + InterfaceName.PhoneNumber.name(), //
                    ACCOUNT_PREFIX + InterfaceName.SalesforceAccountID.name() //

            };
            fieldDisplayNames = new String[] { "Account Id", "Company Name", "Website", "Street", "City", "State",
                    "Zip", "Country", "Phone", "Salesforce Id" };
        } else if (exportType == ExportType.CONTACT) {
            fields = new String[] { //
                    CONTACT_PREFIX + InterfaceName.ContactId.name(), //
                    CONTACT_PREFIX + InterfaceName.ContactName.name(), //
                    CONTACT_PREFIX + InterfaceName.Email.name(), //
                    CONTACT_PREFIX + InterfaceName.PhoneNumber.name(), //
                    CONTACT_PREFIX + InterfaceName.AccountId.name(), //
            };
            fieldDisplayNames = new String[] { "Contact Id", "Contact Name", "Email", "Contact Phone", "Account Id" };
        } else if (exportType == ExportType.ACCOUNT_AND_CONTACT) {
            fields = new String[] { //
                    CONTACT_PREFIX + InterfaceName.ContactId.name(), //
                    CONTACT_PREFIX + InterfaceName.ContactName.name(), //
                    CONTACT_PREFIX + InterfaceName.Email.name(), //
                    CONTACT_PREFIX + InterfaceName.PhoneNumber.name(), //
                    CONTACT_PREFIX + InterfaceName.AccountId.name(), //
                    //
                    ACCOUNT_PREFIX + InterfaceName.LDC_Name.name(), //
                    ACCOUNT_PREFIX + InterfaceName.Website.name(), //
                    ACCOUNT_PREFIX + InterfaceName.Address_Street_1.name().toLowerCase(), //
                    ACCOUNT_PREFIX + InterfaceName.City.name(), //
                    ACCOUNT_PREFIX + InterfaceName.State.name(), //
                    ACCOUNT_PREFIX + InterfaceName.PostalCode.name(), //
                    ACCOUNT_PREFIX + InterfaceName.Country.name(), //
                    ACCOUNT_PREFIX + InterfaceName.SalesforceAccountID.name() //

            };
            fieldDisplayNames = new String[] { "Contact Id", "Contact Name", "Email", "Contact Phone", "Account Id",
                    "Company Name", "Website", "Street", "City", "State", "Zip", "Country", "Salesforce Id" };
        }

        int i = 0;
        for (String field : fields) {
            Attribute attribute = new Attribute();
            attribute.setName(field);
            attribute.setDisplayName(fieldDisplayNames[i++]);
            attribute.setSourceLogicalDataType("");
            attribute.setPhysicalDataType(Type.STRING.name());
            segmentExportTable.addAttribute(attribute);
        }

        String tableName = UUID.randomUUID().toString();
        segmentExportTable.setName(tableName);
        segmentExportTable.setTableType(TableType.DATATABLE);

        segmentExportTable.setDisplayName(metadataSegmentExportJob.getFileName());
        segmentExportTable.setInterpretation(SchemaInterpretation.SalesforceAccount.name());
        segmentExportTable.setTenant(MultiTenantContext.getTenant());
        segmentExportTable.setTenantId(MultiTenantContext.getTenant().getPid());
        segmentExportTable.setMarkedForPurge(false);
        metadataProxy.createTable(customerSpace.toString(), tableName, segmentExportTable);
        segmentExportTable = metadataProxy.getTable(customerSpace.toString(), tableName);

        metadataSegmentExportJob.setTableName(segmentExportTable.getName());

        return metadataSegmentExportJob;
    }

    private MetadataSegmentExport submitExportWorkflowJob(MetadataSegmentExport metadataSegmentExportJob) {
        ApplicationId applicationId = segmentExportWorkflowSubmitter.submit(metadataSegmentExportJob);
        metadataSegmentExportJob.setApplicationId(applicationId.toString());
        return updateSegmentExportJob(metadataSegmentExportJob);
    }

}
