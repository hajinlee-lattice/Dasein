package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.pls.service.HdfsFileDownloader;

public class HdfsFileDownloaderImpl implements HdfsFileDownloader {

    private Configuration yarnConfiguration;

    private String modelingServiceHdfsBaseDir;

    private String filePath;

    public HdfsFileDownloaderImpl(DownloadBuilder builder) {
        this.yarnConfiguration = builder.yarnConfiguration;
        this.modelingServiceHdfsBaseDir = builder.modelingServiceHdfsBaseDir;
    }

    @Override
    public String getFileContents(String tenantId, String modelId, String filter) throws Exception {
        filePath = getFilePath(tenantId, modelId, filter);
        return HdfsUtils.getHdfsFileContents(yarnConfiguration, filePath);
    }

    private String getFilePath(String tenantId, String modelId, final String filter) throws Exception {
        String customer = tenantId;
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

        String singularIdPath = modelingServiceHdfsBaseDir + CustomerSpace.parse(customer).getTenantId() + "/models/";
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

    public static class DownloadBuilder {

        private Configuration yarnConfiguration;

        private String modelingServiceHdfsBaseDir;

        public DownloadBuilder setYarnConfiguration(Configuration yarnConfiguration) {
            this.yarnConfiguration = yarnConfiguration;
            return this;
        }

        public DownloadBuilder setModelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            this.modelingServiceHdfsBaseDir = modelingServiceHdfsBaseDir;
            return this;
        }
    }

}
