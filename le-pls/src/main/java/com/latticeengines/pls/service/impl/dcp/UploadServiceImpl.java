package com.latticeengines.pls.service.impl.dcp;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;

import com.google.common.base.Preconditions;
import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadFileDownloadConfig;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.pls.service.AbstractFileDownloadService;
import com.latticeengines.pls.service.FileDownloadService;
import com.latticeengines.pls.service.dcp.UploadService;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;

@Service
public class UploadServiceImpl extends AbstractFileDownloadService<UploadFileDownloadConfig> implements UploadService {

    private static final Logger log = LoggerFactory.getLogger(UploadServiceImpl.class);

    @Inject
    private FileDownloadService fileDownloadService;

    @Inject
    private UploadProxy uploadProxy;

    @Inject
    private ImportFromS3Service importFromS3Service;

    @Inject
    private Configuration yarnConfiguration;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Override
    public List<UploadDetails> getAllBySourceId(String sourceId, Upload.Status status) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return uploadProxy.getUploads(customerSpace, sourceId, status);
    }

    @Override
    public UploadDetails getByUploadId(String uploadId) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return uploadProxy.getUploadByUploadId(customerSpace, uploadId);
    }

    @Override
    public void downloadByConfig(UploadFileDownloadConfig downloadConfig, HttpServletRequest request,
                               HttpServletResponse response)
            throws Exception {
        response.setHeader("Content-Encoding", "gzip");
        response.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + "upload.zip" + "\"");
        String tenantId = MultiTenantContext.getShortTenantId();
        String uploadId = downloadConfig.getUploadId();
        UploadDetails upload = uploadProxy.getUploadByUploadId(tenantId, uploadId);

        Preconditions.checkNotNull(upload, "object should't be null");
        UploadConfig config = upload.getUploadConfig();
        List<String> pathsToDownload = config.getDownloadPaths()
                .stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        Preconditions.checkState(CollectionUtils.isNotEmpty(pathsToDownload),
                String.format("empty settings in upload config for %s", uploadId));

        // the download part will download files in path in UploadConfig: uploadRawFilePath,
        // uploadImportedFilePath, uploadMatchResultPrefix, uploadImportedErrorFilePath.
        // search csv file under these folders recursively, returned paths are absolute
        // from protocol to file name
        /*
        "upload_config": {
        "upload_ts_prefix": "2020-04-22-18-17-04.697",
        "upload_raw_file_path": "dropfolder/4lqucmbu/Projects/Project_powlhlh9/Source/Source_s2kqvrwd/
            upload/2020-04-22-18-17-04.697/RawFile/Account_1_900.csv",
        "upload_match_result_prefix": "/Projects/Project_powlhlh9/Source/Source_s2kqvrwd/
            upload/2020-04-22-18-17-04.697/MatchResult/",
        "upload_imported_error_file_path": "Projects/Project_powlhlh9/Source/Source_s2kqvrwd/
            upload/2020-04-22-18-17-04.697/ImportResult/ImportError/Account_1_900_error.csv"
        }
         */
        List<String> paths = new ArrayList<>();
        String uploadTS = config.getUploadTSPrefix();
        String rawPath = config.getUploadRawFilePath();
        String commonPrefix = rawPath.substring(0, rawPath.indexOf(uploadTS) + uploadTS.length() + 1);
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(useEmr);
        for (String path : pathsToDownload) {
            String specialPart = path.substring(path.indexOf(uploadTS) + uploadTS.length() + 1);
            if (path.endsWith(".csv")) {
                paths.add(pathBuilder.getS3BucketDir(s3Bucket) + pathBuilder.getPathSeparator()
                        + commonPrefix + specialPart);
            } else {
                path = commonPrefix + specialPart;
                final String filter = ".*.csv";
                List<String> filePaths = importFromS3Service.getFilesForDir(path,
                        filename -> {
                            String name = FilenameUtils.getName(filename);
                            return name.matches(filter);
                        });
                paths.addAll(filePaths);
            }
        }

        Preconditions.checkState(CollectionUtils.isNotEmpty(paths),
                String.format("no file in folder for %s", uploadId));

        log.info("download files: " + paths);
        ZipOutputStream zipOut = new ZipOutputStream(new GzipCompressorOutputStream(response.getOutputStream()));
        for (String filePath : paths) {
            Path path = new Path(filePath);
            FileSystem system = path.getFileSystem(yarnConfiguration);
            InputStream in = system.open(path);
            ZipEntry zipEntry = new ZipEntry(filePath.substring(filePath.lastIndexOf("/") + 1));
            zipOut.putNextEntry(zipEntry);
            try {
                IOUtils.copyLarge(in, zipOut);
            } catch (Exception e ) {
                log.info("unexpected error when copying file: {}", e.getMessage());
            }
            in.close();
            zipOut.closeEntry();
        }
        zipOut.finish();
        zipOut.close();
    }

    @Override
    public String generateToken(String uploadId) {
        UploadFileDownloadConfig config = new UploadFileDownloadConfig();
        config.setUploadId(uploadId);
        return fileDownloadService.generateDownload(config);
    }

}
