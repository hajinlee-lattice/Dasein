package com.latticeengines.pls.service.impl;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.util.ModelIdUtils;

public class HdfsFileHttpDownloader extends AbstractHttpFileDownLoader {

    private String modelId;
    private Configuration yarnConfiguration;
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    private String filter;
    private String modelingServiceHdfsBaseDir;

    private String filePath;

    public HdfsFileHttpDownloader(DownloadRequestBuilder requestBuilder) {
        super(requestBuilder.mimeType);
        this.filter = requestBuilder.filter;
        this.modelId = requestBuilder.modelId;
        this.yarnConfiguration = requestBuilder.yarnConfiguration;
        this.modelSummaryEntityMgr = requestBuilder.modelSummaryEntityMgr;
        this.modelingServiceHdfsBaseDir = requestBuilder.modelingServiceHdfsBaseDir;
    }

    @Override
    protected String getFileName() throws Exception {
        filePath = getFilePath();
        return StringUtils.substringAfterLast(filePath, "/");
    }

    @Override
    protected InputStream getFileInputStream() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);
        return fs.open(new Path(filePath));
    }

    public String getFileContents() throws Exception {
        getFileName();
        return IOUtils.toString(getFileInputStream(), "UTF-8");
    }

    private String getFilePath() throws Exception {
        ModelSummary summary = modelSummaryEntityMgr.findValidByModelId(modelId);
        String customer = summary.getTenant().getId();
        final String uuid = ModelIdUtils.extractUuid(modelId);

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

        String singularIdPath = modelingServiceHdfsBaseDir + customer + "/models/";
        List<String> paths = new ArrayList<>();
        if (HdfsUtils.fileExists(yarnConfiguration, singularIdPath)) {
            paths.addAll(HdfsUtils.getFilesForDirRecursive(yarnConfiguration, singularIdPath, fileFilter));
        }

        customer = CustomerSpace.parse(customer).toString();
        String tupleIdPath = modelingServiceHdfsBaseDir + customer + "/models/";
        if (HdfsUtils.fileExists(yarnConfiguration, tupleIdPath)) {
            paths.addAll(HdfsUtils.getFilesForDirRecursive(yarnConfiguration, tupleIdPath, fileFilter));
        }
        if (CollectionUtils.isEmpty(paths)) {
            throw new LedpException(LedpCode.LEDP_18023);
        }
        return paths.get(0);
    }

    public static class DownloadRequestBuilder {

        private String mimeType;
        private String modelId;
        private Configuration yarnConfiguration;
        private ModelSummaryEntityMgr modelSummaryEntityMgr;
        private String filter;
        private String modelingServiceHdfsBaseDir;

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

        public DownloadRequestBuilder setModelSummaryEntityMgr(ModelSummaryEntityMgr modelSummaryEntityMgr) {
            this.modelSummaryEntityMgr = modelSummaryEntityMgr;
            return this;
        }

        public DownloadRequestBuilder setModelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            this.modelingServiceHdfsBaseDir = modelingServiceHdfsBaseDir;
            return this;
        }
    }
}
