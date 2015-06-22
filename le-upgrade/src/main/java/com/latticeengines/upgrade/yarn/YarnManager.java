package com.latticeengines.upgrade.yarn;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

@Component
public class YarnManager {

    protected static final String UPGRADE_EVENT_TABLE = "NoEventTableForUpgradedModel";
    protected static final String UPGRADE_CONTAINER_ID = "1430367698445_0045";

    protected Configuration yarnConfiguration;
    protected String customerBase;

    private String findModelPath(String customer, String uuid) throws Exception {
        List<String> paths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, customerBase + "/" + customer
                + "/models", new HdfsUtils.HdfsFileFilter() {
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
