package com.latticeengines.serviceflows.workflow.util;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;

public class ImportExportRequest {

    private static final Logger log = LoggerFactory.getLogger(ImportExportRequest.class);

    public String srcPath;
    public String tgtPath;
    public String tableName;
    public boolean hasDataUnit;
    public boolean isSync;
    public boolean isImportFolder;
    public boolean isExportFolder;
    public DataUnit.DataFormat dataFormat;

    public ImportExportRequest() {
    }

    public ImportExportRequest(String srcPath, String tgtPath) {
        super();
        this.srcPath = srcPath;
        this.tgtPath = tgtPath;
    }

    public ImportExportRequest(String srcPath, String tgtPath, String tableName, boolean hasDataUnit, boolean isSync,
            boolean isImportFolder) {
        super();
        this.srcPath = srcPath;
        this.tgtPath = tgtPath;
        this.tableName = tableName;
        this.hasDataUnit = hasDataUnit;
        this.isSync = isSync;
        this.isImportFolder = isImportFolder;
    }

    public ImportExportRequest(String srcPath, String tgtPath, String tableName, boolean hasDataUnit,
            boolean isExportFolder) {
        super();
        this.srcPath = srcPath;
        this.tgtPath = tgtPath;
        this.tableName = tableName;
        this.hasDataUnit = hasDataUnit;
        this.isExportFolder = isExportFolder;
    }

    public static ImportExportRequest exportAtlasTable( //
            String customer, Table table, //
            HdfsToS3PathBuilder pathBuilder, String s3Bucket, String podId, //
            Configuration yarnConfiguration, //
            DataUnit.DataFormat dataFormat, //
            Predicate<FileStatus> fileStatusFilter) {

        String tableName = table.getName();
        List<Extract> extracts = table.getExtracts();
        if (CollectionUtils.isEmpty(extracts) || StringUtils.isBlank(extracts.get(0).getPath())) {
            log.warn("Can not find extracts of the table=" + tableName + " for tenant=" + customer);
            return null;
        }
        String srcDir = pathBuilder.getFullPath(extracts.get(0).getPath());
        FileStatus fileStatus;
        try {
            fileStatus = HdfsUtils.getFileStatus(yarnConfiguration, srcDir);
        } catch (IOException e) {
            log.warn("Can not get time stamp of table=" + tableName + " for tenant=" + customer + " error="
                    + e.getMessage());
            return null;
        }
        if (fileStatusFilter.test(fileStatus)) {
            String tenantId = CustomerSpace.parse(customer).getTenantId();
            String tgtDir = pathBuilder.convertAtlasTableDir(srcDir, podId, tenantId, s3Bucket);
            ImportExportRequest request = new ImportExportRequest(srcDir, tgtDir, tableName, true, true);
            request.dataFormat = dataFormat;
            return request;
        } else {
            log.warn("Hdfs file status does not pass the predicate. Won't export " + tableName + " to S3.");
            return null;
        }
    }

}
