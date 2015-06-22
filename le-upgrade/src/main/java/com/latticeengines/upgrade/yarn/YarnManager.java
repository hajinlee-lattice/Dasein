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

@Component
public class YarnManager {

    protected static final String UPGRADE_EVENT_TABLE = "NoEventTableForUpgradedModel";
    protected static final String UPGRADE_CONTAINER_ID = "1430367698445_0045";

    @Autowired
    protected Configuration yarnConfiguration;

    @Value("${dataplatform.customer.basedir}")
    protected String customerBase;

    public void deleteTupleIdCustomerRoot(String customer) throws Exception {
        String customerPath = YarnPathUtils.constructTupleIdCustomerRoot(customerBase, customer);
        try {
            HdfsUtils.rmdir(yarnConfiguration, customerPath);
        } catch (Exception e) {
            // ignore
        }
    }

    public void createTupleIdCustomerRootIfNotExist(String customer) throws Exception {
        String customerPath = YarnPathUtils.constructTupleIdCustomerRoot(customerBase, customer);
        if (!customerTupleIdPathExists(customer)) {
            HdfsUtils.mkdir(yarnConfiguration, customerPath);
        }
    }

    private boolean customerTupleIdPathExists(String customer) throws Exception {
        String customerPath = YarnPathUtils.constructTupleIdCustomerRoot(customerBase, customer);
        return HdfsUtils.fileExists(yarnConfiguration, customerPath);
    }

    public void copyCustomerFromSingularToTupleId(String customer) throws Exception {
        String srcRoot = YarnPathUtils.constructSingularIdCustomerRoot(customerBase, customer);
        String destRoot = YarnPathUtils.constructTupleIdCustomerRoot(customerBase, customer);

        if (!HdfsUtils.fileExists(yarnConfiguration, srcRoot)) {
            throw new IllegalStateException(String.format("The source path %s does not exist.", srcRoot));
        }

        if (HdfsUtils.fileExists(yarnConfiguration, destRoot)) {
            throw new IllegalStateException(String.format("The destination path %s already exists.", destRoot));
        }

        copyHdfsToHdfs(srcRoot, destRoot);
    }

    public void copyModelFromSingularToTupleId(String customer, String modelGuid) throws Exception {
        String srcRoot = YarnPathUtils.constructSingularIdModelsRoot(customerBase, customer);
        String destRoot = YarnPathUtils.constructTupleIdModelsRoot(customerBase, customer);

        if (!HdfsUtils.fileExists(yarnConfiguration, srcRoot)) {
            throw new IllegalStateException(String.format("The source path %s does not exist.", srcRoot));
        }

        String uuid = YarnPathUtils.extractUuid(modelGuid);
        String srcModelJsonFullPath = findModelPath(customer, uuid);
        String eventTable = YarnPathUtils.parseEventTable(srcModelJsonFullPath);
        String containerId = YarnPathUtils.parseContainerId(srcModelJsonFullPath);
        srcRoot += "/" + eventTable + "/" + uuid + "/" + containerId;
        destRoot += "/" + eventTable + "/" + uuid + "/" + containerId;

        try {
            HdfsUtils.rmdir(yarnConfiguration, destRoot);
        } catch (Exception e) {
            // ignore
        }

        copyHdfsToHdfs(srcRoot, destRoot);
    }

    private void copyHdfsToHdfs(String src, String dest) throws Exception {
        String tmpLocalDir = "tmp" + UUID.randomUUID();
        HdfsUtils.copyHdfsToLocal(yarnConfiguration, src, tmpLocalDir);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, tmpLocalDir, dest);
        FileUtils.deleteQuietly(new File(tmpLocalDir));
    }

    public String findAvaiableEventData(String customer) throws Exception {
        String dataRoot = YarnPathUtils.constructTupleIdDataRoot(customerBase, customer);

        if (!HdfsUtils.fileExists(yarnConfiguration, dataRoot)) {
            throw new IllegalStateException(String.format("The data path %s does not exist.", dataRoot));
        }

        List<String> eventsWithData = HdfsUtils.getFilesForDir(yarnConfiguration, dataRoot);
        if (eventsWithData != null && !eventsWithData.isEmpty()) {
            return YarnPathUtils.parseEventTable(eventsWithData.get(0));
        } else {
            return null;
        }
    }

    private String findModelPath(String customer, String uuid) throws Exception {
        List<String> paths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, customerBase + "/" + customer
                + "/models", new HdfsUtils.HdfsFileFilter() {
            @Override
            public boolean accept(FileStatus fileStatus) {
                if (fileStatus == null) {
                    return false;
                }
                Pattern p = Pattern.compile(".*model.json");
                Matcher matcher = p.matcher(fileStatus.getPath().getName());
                return matcher.matches();
            }
        });
        for (String path : paths) {
            if (path.contains(uuid))
                return path;
        }
        return null;
    }

//    public String constructTupleIdModelDir(String dlTenantName, String modelGuid) throws Exception {
//        String tupleId = CustomerSpace.parse(dlTenantName).toString();
//        String uuid = StringUtils.remove(modelGuid, "ms__").substring(0, 36);
//        String modelDir = customerBase + "/" + tupleId + "/models/" + UPGRADE_EVENT_TABLE + "/" + uuid
//                + "/" + UPGRADE_CONTAINER_ID + "/";
//        return modelDir;
//    }
//
//    private void uploadModelToHdfs(String dlTenantName, String modelGuid, String modelContent) throws Exception{
//        String path = constructTupleIdModelDir(dlTenantName, modelGuid) + "model.json";
//        HdfsUtils.writeToFile(yarnConfiguration, path, modelContent);
//    }
}
