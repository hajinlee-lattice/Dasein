package com.latticeengines.pls.service.impl;

import java.io.InputStream;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;

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

    private String getFilePath() throws Exception {

        ModelSummary summary = modelSummaryEntityMgr.findValidByModelId(modelId);
        String lookupId = summary.getLookupId();
        String tokens[] = lookupId.split("\\|");

        // HDFS file path: <baseDir>/<tenantName>/models/<tableName>/<uuid>
        final String uuid = extractUuid(tokens[2]);
        HdfsUtils.HdfsFileFilter fileFilter = new HdfsUtils.HdfsFileFilter() {
            @Override
            public boolean accept(FileStatus file) {
                if (file == null) {
                    return false;
                }

                String name = file.getPath().getName().toString();
                return name.contains(uuid) && name.matches(filter);
            }
        };

        List<String> paths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, modelingServiceHdfsBaseDir, fileFilter);
        if (CollectionUtils.isEmpty(paths)) {
            throw new LedpException(LedpCode.LEDP_18023);
        }
        return paths.get(0);
    }

    private static String extractUuid(String modelGuid) {
        Pattern pattern = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
        Matcher matcher = pattern.matcher(modelGuid);
        if (matcher.find()) {
            return matcher.group(0);
        }
        throw new IllegalArgumentException("Cannot find uuid pattern in the model GUID " + modelGuid);
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
