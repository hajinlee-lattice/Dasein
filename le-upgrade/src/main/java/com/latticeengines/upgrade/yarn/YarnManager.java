package com.latticeengines.upgrade.yarn;

import java.io.File;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

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

    public void copyCustomerFromSingularToTupleId(String customer) throws Exception {
        String srcRoot = YarnPathUtils.constructSingularIdCustomerRoot(customerBase, customer);
        String destRoot = YarnPathUtils.constructTupleIdCustomerRoot(customerBase, customer);

        if (!HdfsUtils.fileExists(yarnConfiguration, srcRoot)) {
            throw new IllegalStateException(String.format("The source path %s does not exist.", srcRoot));
        }

        if (HdfsUtils.fileExists(yarnConfiguration, destRoot)) {
            throw new IllegalStateException(String.format("The destination path %s already exists.", destRoot));
        }

        String tmpLocalDir = "tmp" + customer;
        HdfsUtils.copyHdfsToLocal(yarnConfiguration, srcRoot, tmpLocalDir);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, tmpLocalDir, destRoot);
        FileUtils.deleteQuietly(new File(tmpLocalDir));
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

    public String constructTupleIdModelDir(String dlTenantName, String modelGuid) throws Exception {
        String tupleId = CustomerSpace.parse(dlTenantName).toString();
        String uuid = StringUtils.remove(modelGuid, "ms__").substring(0, 36);
        String modelDir = customerBase + "/" + tupleId + "/models/" + UPGRADE_EVENT_TABLE + "/" + uuid
                + "/" + UPGRADE_CONTAINER_ID + "/";
        return modelDir;
    }

    private void uploadModelToHdfs(String dlTenantName, String modelGuid, String modelContent) throws Exception{
        String path = constructTupleIdModelDir(dlTenantName, modelGuid) + "model.json";
        HdfsUtils.writeToFile(yarnConfiguration, path, modelContent);
    }
}
