package com.latticeengines.app.exposed.download;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;

public class HdfsFileHttpDownloader extends AbstractHttpFileDownLoader {

    private static final Logger log = LoggerFactory.getLogger(HdfsFileHttpDownloader.class);

    private String modelId;
    private Configuration yarnConfiguration;
    private ModelSummaryProxy modelSummaryProxy;

    private String filter;
    private String modelingServiceHdfsBaseDir;

    private String customer;
    private String filePath;

    // for test purpose
    protected HdfsFileHttpDownloader() {
        super(null, null, null, null);
    }

    public HdfsFileHttpDownloader(DownloadRequestBuilder requestBuilder) {
        super(requestBuilder.mimeType, requestBuilder.importFromS3Service, requestBuilder.cdlAttrConfigProxy,
                requestBuilder.batonService);
        this.filter = requestBuilder.filter;
        this.modelId = requestBuilder.modelId;
        this.yarnConfiguration = requestBuilder.yarnConfiguration;
        this.modelingServiceHdfsBaseDir = requestBuilder.modelingServiceHdfsBaseDir;
        this.modelSummaryProxy = requestBuilder.modelSummaryProxy;
    }

    @Override
    protected String getFileName() throws Exception {
        filePath = getFilePath();
        return StringUtils.substringAfterLast(filePath, "/");
    }

    @Override
    protected InputStream getFileInputStream() throws Exception {
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(yarnConfiguration);
        return fs.open(new Path(filePath));
    }

    public String getFileContents() throws Exception {
        getFileName();
        return IOUtils.toString(getFileInputStream(), "UTF-8");
    }

    private String getFilePath() throws Exception {
        ModelSummary summary = modelSummaryProxy.findValidByModelId(MultiTenantContext.getTenant().getId(), modelId);
        String s3FilePath = searchS3FilePath(summary);
        if (StringUtils.isNotBlank(s3FilePath)) {
            log.info("Download from S3 path=" + s3FilePath);
            return s3FilePath;
        }
        String hdfsPath = searchHdfsFilePath(summary);
        log.info("Download from HDFS path=" + hdfsPath);
        return hdfsPath;
    }

    private String searchHdfsFilePath(ModelSummary summary) throws IOException {
        customer = summary.getTenant().getId();
        final String uuid = UuidUtils.extractUuid(modelId);

        // HDFS file path: <baseDir>/<tenantName>/models/<tableName>/<uuid>
        HdfsUtils.HdfsFileFilter fileFilter = new HdfsUtils.HdfsFileFilter() {
            @Override
            public boolean accept(FileStatus file) {
                if (file == null) {
                    return false;
                }
                String name = file.getPath().getName();
                String path = file.getPath().toString();
                return name.matches(filter) && path.contains(uuid);
            }
        };

        String eventTableName = "";
        if (StringUtils.isNotEmpty(summary.getEventTableName())) {
            eventTableName = summary.getEventTableName();
        }

        customer = CustomerSpace.parse(customer).getTenantId();
        String singularIdPath = modelingServiceHdfsBaseDir + customer + "/models/" + eventTableName;
        List<String> paths = new ArrayList<>();
        if (HdfsUtils.fileExists(yarnConfiguration, singularIdPath)) {
            paths.addAll(HdfsUtils.getFilesForDirRecursive(yarnConfiguration, singularIdPath, fileFilter));
        }
        if (CollectionUtils.isNotEmpty(paths)) {
            return paths.get(0);
        }

        customer = CustomerSpace.parse(customer).toString();
        String tupleIdPath = modelingServiceHdfsBaseDir + customer + "/models/" + eventTableName;
        if (HdfsUtils.fileExists(yarnConfiguration, tupleIdPath)) {
            paths.addAll(HdfsUtils.getFilesForDirRecursive(yarnConfiguration, tupleIdPath, fileFilter));
        }
        if (CollectionUtils.isNotEmpty(paths)) {
            if (StringUtils.isNotEmpty(summary.getApplicationId())) {
                String applicationIdDirectory = ApplicationIdUtils.stripJobId(summary.getApplicationId());
                Optional<String> completedModelingPath = paths.stream()
                        .filter(path -> path.contains(applicationIdDirectory)).findFirst();
                return completedModelingPath.orElseGet(() -> paths.get(0));
            }
            return paths.get(0);
        }

        String postMatchEventTablePath = modelingServiceHdfsBaseDir + customer + "/data/" + eventTableName
                + "/csv_files/";
        if (HdfsUtils.fileExists(yarnConfiguration, postMatchEventTablePath)) {
            paths.addAll(HdfsUtils.getFilesForDir(yarnConfiguration, postMatchEventTablePath, filter));
        }
        if (CollectionUtils.isNotEmpty(paths)) {
            return paths.get(0);
        }

        String eventTableScoreFilePath = modelingServiceHdfsBaseDir + customer + "/data/" + eventTableName
                + "/csv_files/score_event_table_output";
        if (HdfsUtils.fileExists(yarnConfiguration, eventTableScoreFilePath)) {
            paths.addAll(HdfsUtils.getFilesForDir(yarnConfiguration, eventTableScoreFilePath, filter));
        }
        if (CollectionUtils.isEmpty(paths)) {
            throw new LedpException(LedpCode.LEDP_18023);
        }
        return paths.get(0);
    }

    private String searchS3FilePath(ModelSummary summary) {
        customer = summary.getTenant().getId();
        final String uuid = UuidUtils.extractUuid(modelId);

        // HDFS file path: <baseDir>/<tenantName>/models/<tableName>/<uuid>
        HdfsUtils.HdfsFilenameFilter fileFilter = new HdfsUtils.HdfsFilenameFilter() {
            @Override
            public boolean accept(String filePath) {
                if (filePath == null) {
                    return false;
                }
                String name = FilenameUtils.getName(filePath);
                String path = FilenameUtils.getFullPath(filePath);
                return name.matches(filter) && path.contains(uuid);
            }
        };

        String eventTableName = "";
        if (StringUtils.isNotEmpty(summary.getEventTableName())) {
            eventTableName = summary.getEventTableName();
        }

        CustomerSpace space = CustomerSpace.parse(customer);
        customer = space.toString();
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder();
        String tupleIdPath = pathBuilder.getS3AnalyticsModelTableDir(importFromS3Service.getS3Bucket(),
                space.getTenantId(), eventTableName);

        List<String> paths = importFromS3Service.getFilesForDir(tupleIdPath, fileFilter);
        if (CollectionUtils.isNotEmpty(paths)) {
            if (StringUtils.isNotEmpty(summary.getApplicationId())) {
                String applicationIdDirectory = ApplicationIdUtils.stripJobId(summary.getApplicationId());
                Optional<String> completedModelingPath = paths.stream()
                        .filter(path -> path.contains(applicationIdDirectory)).findFirst();
                return completedModelingPath.isPresent() ? completedModelingPath.get() : paths.get(0);
            }
            return paths.get(0);
        }

        String postMatchEventTablePath = pathBuilder.getS3AnalyticsDataTableDir(importFromS3Service.getS3Bucket(),
                space.getTenantId(), eventTableName) + "/csv_files/";
        fileFilter = new HdfsUtils.HdfsFilenameFilter() {
            @Override
            public boolean accept(String filePath) {
                if (filePath == null) {
                    return false;
                }
                String name = FilenameUtils.getName(filePath);
                return name.matches(filter);
            }
        };
        paths = importFromS3Service.getFilesForDir(postMatchEventTablePath, fileFilter);
        if (CollectionUtils.isNotEmpty(paths)) {
            return paths.get(0);
        }
        return null;
    }

    public static class DownloadRequestBuilder {

        private String mimeType;
        private String modelId;
        private Configuration yarnConfiguration;
        private String filter;
        private String modelingServiceHdfsBaseDir;
        private ModelSummaryProxy modelSummaryProxy;
        protected ImportFromS3Service importFromS3Service;
        private CDLAttrConfigProxy cdlAttrConfigProxy;
        private BatonService batonService;

        public DownloadRequestBuilder setMimeType(String mimeType) {
            this.mimeType = mimeType;
            return this;
        }

        public DownloadRequestBuilder setFilter(String filter) {
            this.filter = filter;
            return this;
        }

        public DownloadRequestBuilder setModelId(String modelId) {
            this.modelId = modelId;
            return this;
        }

        public DownloadRequestBuilder setYarnConfiguration(Configuration yarnConfiguration) {
            this.yarnConfiguration = yarnConfiguration;
            return this;
        }

        public DownloadRequestBuilder setModelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            this.modelingServiceHdfsBaseDir = modelingServiceHdfsBaseDir;
            return this;
        }

        public DownloadRequestBuilder setModelSummaryProxy(ModelSummaryProxy modelSummaryProxy) {
            this.modelSummaryProxy = modelSummaryProxy;
            return this;
        }

        public DownloadRequestBuilder setImportFromS3Service(ImportFromS3Service importFromS3Service) {
            this.importFromS3Service = importFromS3Service;
            return this;
        }

        public DownloadRequestBuilder setCDLAttrConfigProxy(CDLAttrConfigProxy cdlAttrConfigProxy) {
            this.cdlAttrConfigProxy = cdlAttrConfigProxy;
            return this;
        }

        public DownloadRequestBuilder setBatonService(BatonService batonService) {
            this.batonService = batonService;
            return this;
        }
    }
}
