package com.latticeengines.upgrade.yarn;

import java.io.File;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("yarnManager")
public class YarnManager {

    @Autowired
    protected Configuration yarnConfiguration;

    @Value("${dataplatform.customer.basedir}")
    protected String customerBase;

    public void deleteTupleIdCustomerRoot(String customer) {
        String customerPath = YarnPathUtils.constructTupleIdCustomerRoot(customerBase, customer);
        try {
            HdfsUtils.rmdir(yarnConfiguration, customerPath);
        } catch (Exception e) {
            // ignore
        }
    }

    public void createTupleIdCustomerRootIfNotExist(String customer) {
        String customerPath = YarnPathUtils.constructTupleIdCustomerRoot(customerBase, customer);
        if (!customerTupleIdPathExists(customer)) {
            try {
                HdfsUtils.mkdir(yarnConfiguration, customerPath);
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_24000,
                        "Cannot create tuple id directory for customer " + customer, e);
            }
        }
    }

    private boolean customerTupleIdPathExists(String customer) {
        String customerPath = YarnPathUtils.constructTupleIdCustomerRoot(customerBase, customer);
        return hdfsPathExists(customerPath);
    }

    public void copyCustomerFromSingularToTupleId(String customer) {
        String srcRoot = YarnPathUtils.constructSingularIdCustomerRoot(customerBase, customer);
        String destRoot = YarnPathUtils.constructTupleIdCustomerRoot(customerBase, customer);

        if (!hdfsPathExists(srcRoot)) {
            throw new IllegalStateException(String.format("The source path %s does not exist.", srcRoot));
        }

        if (hdfsPathExists(destRoot)) {
            throw new IllegalStateException(String.format("The destination path %s already exists.", destRoot));
        }

        copyHdfsToHdfs(srcRoot, destRoot);
    }

    public void copyModelFromSingularToTupleId(String customer, String modelGuid) {
        String srcRoot = YarnPathUtils.constructSingularIdModelsRoot(customerBase, customer);
        String destRoot = YarnPathUtils.constructTupleIdModelsRoot(customerBase, customer);

        if (!hdfsPathExists(srcRoot)) {
            throw new IllegalStateException(String.format("The source path %s does not exist.", srcRoot));
        }

        String uuid = YarnPathUtils.extractUuid(modelGuid);
        String srcModelJsonFullPath = findModelPath(customer, uuid);
        String eventTable = YarnPathUtils.parseEventTable(srcModelJsonFullPath);
        String containerId = YarnPathUtils.parseContainerId(srcModelJsonFullPath);
        srcRoot += "/" + eventTable + "/" + uuid + "/" + containerId;
        destRoot += "/" + eventTable + "/" + uuid + "/" + containerId;

        copyHdfsToHdfsWithDestCleared(srcRoot, destRoot);
    }

    public void copyDataFromSingularToTupleId(String customer) {
        String srcRoot = YarnPathUtils.constructSingularIdDataRoot(customerBase, customer);
        String destRoot = YarnPathUtils.constructTupleIdDataRoot(customerBase, customer);

        if (!hdfsPathExists(srcRoot)) {
            throw new IllegalStateException(String.format("The data path %s does not exist.", srcRoot));
        }

        String eventTableWithData = findAvaiableEventData(customer);
        if (eventTableWithData != null) {
            String src = srcRoot + "/" + eventTableWithData;
            String dest = destRoot + "/" + eventTableWithData;
            copyHdfsToHdfsWithDestCleared(src, dest);

            src = srcRoot + "/EventMetadata";
            dest = destRoot + "/EventMetadata";
            copyHdfsToHdfsWithDestCleared(src, dest);
        } else {
            throw new IllegalStateException(String.format("Customer %s does not have data.", customer));
        }
    }

    public boolean srcModelPathExists(String customer, String modelGuid) {
        String uuid = YarnPathUtils.extractUuid(modelGuid);
        try {
            String srcModelJsonFullPath = findModelPath(customer, uuid);
            return srcModelJsonFullPath != null;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean destModelPathExists(String customer, String modelGuid) {
        String uuid = YarnPathUtils.extractUuid(modelGuid);
        String srcModelJsonFullPath = findModelPath(customer, uuid);
        String eventTable = YarnPathUtils.parseEventTable(srcModelJsonFullPath);
        String containerId = YarnPathUtils.parseContainerId(srcModelJsonFullPath);
        String destPath = YarnPathUtils.constructTupleIdModelsRoot(customerBase, customer)
                + "/" + eventTable + "/" + uuid + "/" + containerId;
        return hdfsPathExists(destPath);
    }

    public boolean modelSummaryExists(String customer, String modelGuid) {
        String uuid = YarnPathUtils.extractUuid(modelGuid);
        String srcModelJsonFullPath = findModelPath(customer, uuid);
        String eventTable = YarnPathUtils.parseEventTable(srcModelJsonFullPath);
        String containerId = YarnPathUtils.parseContainerId(srcModelJsonFullPath);
        String destPath = YarnPathUtils.constructSingularIdModelsRoot(customerBase, customer)
                + "/" + eventTable + "/" + uuid + "/" + containerId
                + "/enhancements/modelsummary.json";
        return hdfsPathExists(destPath);
    }

    private boolean hdfsPathExists(String path) {
        try {
            return HdfsUtils.fileExists(yarnConfiguration, path);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24000,
                    "Cannot check if path " + path + " exists in hdfs.", e);
        }
    }

    private void copyHdfsToHdfsWithDestCleared(String src, String dest) {
        String tmpLocalDir = "tmp" + UUID.randomUUID();
        try {
            HdfsUtils.copyHdfsToLocal(yarnConfiguration, src, tmpLocalDir);
            HdfsUtils.copyLocalToHdfs(yarnConfiguration, tmpLocalDir, dest);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24000, "Failed to copy file from one src to dest path.", e);
        } finally {
            FileUtils.deleteQuietly(new File(tmpLocalDir));
        }
    }

    private void copyHdfsToHdfs(String src, String dest) {
        String tmpLocalDir = "tmp" + UUID.randomUUID();
        try {
            HdfsUtils.copyHdfsToLocal(yarnConfiguration, src, tmpLocalDir);
            HdfsUtils.copyLocalToHdfs(yarnConfiguration, tmpLocalDir, dest);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24000, "Failed to copy file from one src to dest path.", e);
        } finally {
            FileUtils.deleteQuietly(new File(tmpLocalDir));
        }
    }

    private String findAvaiableEventData(String customer) {
        String dataRoot = YarnPathUtils.constructSingularIdDataRoot(customerBase, customer);

        if (!hdfsPathExists(dataRoot)) {
            throw new IllegalStateException(String.format("The data path %s does not exist.", dataRoot));
        }

        try {
            List<String> eventsWithData = HdfsUtils.getFilesForDir(yarnConfiguration, dataRoot);
            if (eventsWithData != null && eventsWithData.size() >= 2) {
                for (String event: eventsWithData) {
                    if (!"EventMetadata".equals(event))
                        return YarnPathUtils.parseEventTable(eventsWithData.get(0));
                }
            }
            return null;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24000, "Cannot find the data folder for customer" + customer, e);
        }
    }

    private String findModelPath(String customer, String uuid) {
        try {
            List<String> paths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, customerBase + "/" + customer
                    + "/models", new HdfsUtils.HdfsFileFilter() {
                @Override
                public boolean accept(FileStatus fileStatus) {
                    if (fileStatus == null) {
                        return false;
                    }
                    Pattern p = Pattern.compile(".*json");
                    Matcher matcher = p.matcher(fileStatus.getPath().getName());
                    return matcher.matches();
                }
            });
            for (String path : paths) {
                if (path.contains(uuid))
                    return path;
            }
            return null;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24000, "Cannot find the path for model" + uuid, e);
        }
    }
}
