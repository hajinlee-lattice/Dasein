package com.latticeengines.upgrade.yarn;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import com.latticeengines.common.exposed.util.HdfsUtils;

public class YarnManager {

    protected static final String UPGRADE_EVENT_TABLE = "NoEventTableForUpgradedModel";
    protected static final String UPGRADE_CONTAINER_ID = "1430367698445_0045";

    protected Configuration yarnConfiguration;
    protected String customerBase;

    public String defaultFs() {
        return yarnConfiguration.get("fs.defaultFS");
    } // this is used to check Autowired

    protected String findModelPath(String customer, String uuid) throws Exception {
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
}
