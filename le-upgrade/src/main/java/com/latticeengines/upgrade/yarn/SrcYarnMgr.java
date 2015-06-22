package com.latticeengines.upgrade.yarn;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;

@Component("srcYarnMgr")
public class SrcYarnMgr extends YarnManager {

    @Autowired
    @Qualifier(value = "src")
    private Configuration srcYarnConfig;

    @Value("${upgarde.src.dp.customer.basedir}")
    protected String srcCustomerBase;

    @PostConstruct
    private void wireUpProperties() {
        this.yarnConfiguration = this.srcYarnConfig;
        this.customerBase = srcCustomerBase;
    }

    public String findModelPath(String customer, String uuid) throws Exception {
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
