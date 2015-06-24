package com.latticeengines.upgrade.yarn;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.latticeengines.domain.exposed.camille.CustomerSpace;
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

    public void copyModelsFromSingularToTupleId(String customer) {
        String srcRoot = YarnPathUtils.constructSingularIdModelsRoot(customerBase, customer);
        String destRoot = YarnPathUtils.constructTupleIdModelsRoot(customerBase, customer);

        if (!hdfsPathExists(srcRoot)) {
            throw new IllegalStateException(String.format("The source path %s does not exist.", srcRoot));
        }

        if (hdfsPathExists(destRoot)) {
            throw new IllegalStateException(String.format("The destination path %s already exists.", destRoot));
        }

        copyHdfsToHdfs(srcRoot, destRoot);
    }

    public void fixModelName(String customer, String modelGuid) {
        String uuid = YarnPathUtils.extractUuid(modelGuid);
        String srcModelJsonFullPath = findModelPath(CustomerSpace.parse(customer).toString(), uuid);
        if (!srcModelJsonFullPath.endsWith("model.json")) {
            String newModelJsonFullPath = srcModelJsonFullPath.replace(".json", "_model.json");
            try {
                HdfsUtils.moveFile(yarnConfiguration, srcModelJsonFullPath, newModelJsonFullPath);
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_24000, "Failed to move file from one src to dest path.", e);
            }
        }
    }

    public boolean modelJsonExistsInSingularId(String customer, String modelGuid) {
        String uuid = YarnPathUtils.extractUuid(modelGuid);
        try {
            String srcModelJsonFullPath = findModelPath(customer, uuid);
            return srcModelJsonFullPath != null;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean modelSummaryExistsInSingularId(String customer, String modelGuid) {
        String uuid = YarnPathUtils.extractUuid(modelGuid);
        String srcModelJsonFullPath = findModelPath(customer, uuid);
        String eventTable = YarnPathUtils.parseEventTable(srcModelJsonFullPath);
        String containerId = YarnPathUtils.parseContainerId(srcModelJsonFullPath);
        String destPath = YarnPathUtils.constructSingularIdModelsRoot(customerBase, customer)
                + "/" + eventTable + "/" + uuid + "/" + containerId
                + "/enhancements/modelsummary.json";
        return hdfsPathExists(destPath);
    }

    public List<String> findAllUuidsInSingularId(String customer) {
        List<String> paths = findAllModelPathsInSingularId(customer);
        List<String> uuids = new ArrayList<>();
        for (String path: paths) {
            uuids.add(YarnPathUtils.parseUuid(path));
        }
        return uuids;
    }

    public void generateModelSummary(String customer, String modelGuid) { }

    private boolean hdfsPathExists(String path) {
        try {
            return HdfsUtils.fileExists(yarnConfiguration, path);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24000,
                    "Cannot check if path " + path + " exists in hdfs.", e);
        }
    }

    private void copyHdfsToHdfs(String src, String dest) {
        String tmpLocalDir = "tmp/" + UUID.randomUUID();
        try {
            HdfsUtils.copyHdfsToLocal(yarnConfiguration, src, tmpLocalDir);
            HdfsUtils.copyLocalToHdfs(yarnConfiguration, tmpLocalDir, dest);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24000, "Failed to copy file from one src to dest path.", e);
        } finally {
            FileUtils.deleteQuietly(new File(tmpLocalDir));
        }
    }

    private String findModelPath(String customer, String uuid) {
        List<String> paths = findAllModelPathsInSingularId(customer);
        for (String path : paths) {
            if (path.contains(uuid))
                return path;
        }
        RuntimeException e = new RuntimeException("No model json with specific uuid can be found.");
        throw new LedpException(LedpCode.LEDP_24000, "Cannot find the path for model" + uuid, e);
    }

    private List<String> findAllModelPathsInSingularId(String customer) {
        String modelsRoot = YarnPathUtils.constructSingularIdModelsRoot(customerBase, customer);
        if (!hdfsPathExists(modelsRoot)) {
            return new ArrayList<>();
        }

        try {
            List<String> paths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, customerBase + "/" + customer
                    + "/models", new HdfsUtils.HdfsFileFilter() {
                @Override
                public boolean accept(FileStatus fileStatus) {
                    if (fileStatus == null) {
                        return false;
                    }
                    Pattern p = Pattern.compile(".*json");
                    String filePath = fileStatus.getPath().getName();
                    Matcher matcher = p.matcher(filePath);
                    return (matcher.matches() && !shouldExclude(filePath));
                }

                private boolean shouldExclude(String path) {
                    List<String> blacklist = Arrays.asList(
                            "enhancements",
                            "modelsummary",
                            "diagnostics",
                            "DataComposition",
                            "ScoreDerivation",
                            "/1/"
                    );
                    for (String token: blacklist) {
                        if (path.contains(token)) return true;
                    }
                    return false;
                }
            });
            return paths;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24000, "Cannot find all model jsons for customer " + customer, e);
        }
    }

}
