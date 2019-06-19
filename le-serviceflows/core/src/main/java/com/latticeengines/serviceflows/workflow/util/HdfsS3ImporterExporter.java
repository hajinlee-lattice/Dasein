package com.latticeengines.serviceflows.workflow.util;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.yarn.configuration.ConfigurationUtils;

import com.latticeengines.camille.exposed.locks.LockManager;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;

public class HdfsS3ImporterExporter implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(HdfsS3ImporterExporter.class);
    private static final String SUCCESS_FILE = "_SUCCESS";

    private String customerSpace;
    private String tenantId;
    private String yarnQueue;
    private Configuration distCpConfiguration;
    private DataUnitProxy dataUnitProxy;

    private String srcPath;
    private String tgtPath;
    private String tableName;
    private boolean hasDataUnit;
    private boolean isSync;
    private boolean isImportFolder;
    private boolean isExportFolder;

    public HdfsS3ImporterExporter(String customerSpace, //
                           Configuration distCpConfiguration, //
                           String yarnQueue, //
                           DataUnitProxy dataUnitProxy,
                           ImportExportRequest request) {
        this.customerSpace = customerSpace;
        this.tenantId = CustomerSpace.parse(customerSpace).getTenantId();
        this.distCpConfiguration = distCpConfiguration;
        this.yarnQueue = yarnQueue;
        this.dataUnitProxy = dataUnitProxy;

        this.srcPath = request.srcPath;
        this.tgtPath = request.tgtPath;
        this.tableName = request.tableName;
        this.hasDataUnit = request.hasDataUnit;
        this.isSync = request.isSync;
        this.isImportFolder = request.isImportFolder;
        this.isExportFolder = request.isExportFolder;
    }

    @Override
    public void run() {
        try (PerformanceTimer timer = new PerformanceTimer(
                "Copying src path=" + srcPath + " to tgt path=" + tgtPath)) {
            try {
                Configuration hadoopConfiguration = createConfiguration();
                if (isSync) {
                    globalSyncCopy(hadoopConfiguration);
                } else {
                    distCpWithRetry(hadoopConfiguration);
                }
                if (hasDataUnit && StringUtils.isNotBlank(tableName)) {
                    registerDataUnit();
                }
                writeSuccessFile();

            } catch (Exception ex) {
                String msg = String.format("Failed to copy src path=%s to tgt path=%s for tenant=%s", srcPath,
                        tgtPath, customerSpace);
                log.error(msg, ex);
                throw new RuntimeException(msg);
            }
        }
    }

    private void writeSuccessFile() throws IOException {
        if (isExportFolder) {
            HdfsUtils.writeToFile(distCpConfiguration, getSuccessFile(srcPath), "success!");
        }
        if (isImportFolder) {
            HdfsUtils.writeToFile(distCpConfiguration, getSuccessFile(tgtPath), "success!");
        }
    }

    private void globalSyncCopy(Configuration hadoopConfiguration) throws Exception {
        String lockName = tenantId + "_" + tableName;
        try {
            LockManager.registerCrossDivisionLock(lockName);
            LockManager.acquireWriteLock(lockName, 360, TimeUnit.MINUTES);
            if (!HdfsUtils.fileExists(distCpConfiguration, tgtPath)) {
                distCpWithRetry(hadoopConfiguration);
            }
        } finally {
            LockManager.releaseWriteLock(lockName);
        }
    }

    private void distCpWithRetry(Configuration hadoopConfiguration) throws Exception {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Retry=" + ctx.getRetryCount() + ") distcp from " + srcPath + " to " + tgtPath);
            }
            HdfsUtils.distcp(hadoopConfiguration, srcPath, tgtPath, yarnQueue);
            return true;
        });
    }

    private Configuration createConfiguration() {
        Configuration hadoopConfiguration = ConfigurationUtils.createFrom(distCpConfiguration, new Properties());
        String jobName = StringUtils.isNotBlank(tableName) ? tenantId + "~" + tableName : tenantId;
        hadoopConfiguration.set(JobContext.JOB_NAME, jobName);
        return hadoopConfiguration;
    }

    private void registerDataUnit() {
        DataUnit existing = dataUnitProxy.getByNameAndType(customerSpace, tableName, DataUnit.StorageType.S3);
        if (existing == null) {
            S3DataUnit unit = new S3DataUnit();
            unit.setTenant(tenantId);
            unit.setName(tableName);
            unit.setLinkedDir(tgtPath);
            DataUnit created = dataUnitProxy.create(customerSpace, unit);
            log.info("Registered DataUnit: " + JsonUtils.pprint(created));
        } else {
            log.info("There is already a data unit for " + tableName + ": " + JsonUtils.pprint(existing));
        }
    }

    private String getSuccessFile(String hdfsPath) {
        return hdfsPath + "/" + SUCCESS_FILE;
    }
}
