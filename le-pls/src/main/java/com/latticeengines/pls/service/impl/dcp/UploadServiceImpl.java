package com.latticeengines.pls.service.impl.dcp;

import java.util.List;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.springframework.stereotype.Service;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;

import com.google.common.base.Preconditions;
import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.common.exposed.util.GzipUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.pls.service.dcp.UploadService;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;

@Service
public class UploadServiceImpl implements UploadService {

    private static final Logger log = LoggerFactory.getLogger(UploadServiceImpl.class);

    @Inject
    private UploadProxy uploadProxy;

    @Inject
    private ImportFromS3Service importFromS3Service;

    @Override
    public List<Upload> getAllBySourceId(String sourceId, Upload.Status status) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return uploadProxy.getUploads(customerSpace, sourceId, status);
    }

    @Override
    public Upload getByUploadId(long uploadPid) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return uploadProxy.getUpload(customerSpace, uploadPid);
    }

    @Override
    public void downloadUpload(String uploadId, HttpServletRequest request, HttpServletResponse response) throws Exception {
        String tenantId = MultiTenantContext.getShortTenantId();
        Upload upload = uploadProxy.getUpload(tenantId, Long.parseLong(uploadId));

        UploadConfig config = upload.getUploadConfig();
        String rawPath = config.getUploadRawFilePath();
        String uploadTSPrefix = config.getUploadTSPrefix();
        int index = rawPath.indexOf(uploadTSPrefix);
        Preconditions.checkState(index != -1, String.format("invalid upload config %s.", uploadId));
        String parentPath = rawPath.substring(0, rawPath.indexOf(uploadTSPrefix));

        final String filter = ".*.csv";
        List<String> files = importFromS3Service.getFilesForDir(parentPath,
                filename -> {
                    String name = FilenameUtils.getName(filename);
                    return name.matches(filter);
                });


        log.info("download files: " + files);
        ZipOutputStream zipOut = new ZipOutputStream(response.getOutputStream());
        for (String file : files) {
            InputStream in = importFromS3Service.getS3FileInputStream(file);
            ZipEntry zipEntry = new ZipEntry(file.substring(file.lastIndexOf("/") + 1));
            zipOut.putNextEntry(zipEntry);
            GzipUtils.copyAndCompressStream(in, zipOut);
            zipOut.closeEntry();
        }
        zipOut.finish();
        zipOut.close();
        response.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        response.setStatus(HttpServletResponse.SC_OK);
        response.addHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + "upload.zip" + "\"");

    }
}
