package com.latticeengines.domain.exposed.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;

public final class TenantCleanupUtils {

    protected TenantCleanupUtils() {
        throw new UnsupportedOperationException();
    }

    private static final String customerBase = "/user/s-analytics/customers";

    public static void cleanupTenantInHdfs(Configuration yarnConfiguration, String contractId, String contractPath) throws IOException {
        String customerSpace = CustomerSpace.parse(contractId).toString();
        if (HdfsUtils.fileExists(yarnConfiguration, contractPath)) {
            HdfsUtils.rmdir(yarnConfiguration, contractPath);
        }
        String customerPath = new Path(customerBase).append(customerSpace).toString();
        if (HdfsUtils.fileExists(yarnConfiguration, customerPath)) {
            HdfsUtils.rmdir(yarnConfiguration, customerPath);
        }
        contractPath = new Path(customerBase).append(contractId).toString();
        if (HdfsUtils.fileExists(yarnConfiguration, contractPath)) {
            HdfsUtils.rmdir(yarnConfiguration, contractPath);
        }
    }
}
