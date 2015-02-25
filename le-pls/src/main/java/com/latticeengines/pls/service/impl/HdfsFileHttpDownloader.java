package com.latticeengines.pls.service.impl;

import java.io.InputStream;
import java.util.List;

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

    public HdfsFileHttpDownloader(String mimeType, String filter) {
        super(mimeType);
        this.filter = filter;
    }

    @Override
    protected String getFileName() {
        try {
            filePath = getFilePath();
            return StringUtils.substringAfterLast(filePath, "/");
            
        } catch (Exception ex) {
            log.error("Failed to download file for modelId:" + modelId, ex);
            throw new LedpException(LedpCode.LEDP_18022, ex);
        }
    }

    @Override
    protected InputStream getFileInputStream() {
        try {
            FileSystem fs = FileSystem.get(yarnConfiguration);
            InputStream is = fs.open(new Path(filePath));
            return is;
            
        } catch (Exception ex) {
            log.error("Failed to download file for modelId:" + modelId, ex);
            throw new LedpException(LedpCode.LEDP_18022, ex);
        }
    }

    public HdfsFileHttpDownloader setModelId(String modelId) {
        this.modelId = modelId;
        return this;
    }

    public HdfsFileHttpDownloader setYarnConfiguration(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
        return this;
    }

    public HdfsFileHttpDownloader setModelSummaryEntityMgr(ModelSummaryEntityMgr modelSummaryEntityMgr) {
        this.modelSummaryEntityMgr = modelSummaryEntityMgr;
        return this;
    }

    public HdfsFileHttpDownloader setModelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
        this.modelingServiceHdfsBaseDir = modelingServiceHdfsBaseDir;
        return this;
    }

    private String getFilePath() throws Exception {

        ModelSummary summary = modelSummaryEntityMgr.findValidByModelId(modelId);
        String lookupId = summary.getLookupId();
        String tokens[] = lookupId.split("\\|");

        StringBuilder pathBuilder = new StringBuilder(modelingServiceHdfsBaseDir).append(tokens[0]).append("/models/");
        pathBuilder.append(tokens[1]).append("/").append(tokens[2]);

        HdfsUtils.HdfsFileFilter fileFilter = new HdfsUtils.HdfsFileFilter() {
            @Override
            public boolean accept(FileStatus file) {
                if (file == null) {
                    return false;
                }

                String name = file.getPath().getName().toString();
                return name.matches(filter);
            }
        };

        List<String> paths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, pathBuilder.toString(), fileFilter);
        return paths.get(0);
    }

}
