package com.latticeengines.serviceflows.workflow.util;

import java.io.IOException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;

public final class ScalingUtils {

    private static final Logger log = LoggerFactory.getLogger(ScalingUtils.class);

    private static final double GB = 1024. * 1024 * 1024;

    /**
     * 8G -> 2
     * 24G -> 3
     * 72G -> 4
     */
    public static int getMultiplier(double sizeInGb) {
        int multiplier = 1;
        if (sizeInGb >= 72) {
            multiplier = 4;
        } else if (sizeInGb >= 24) {
            multiplier = 3;
        } else if (sizeInGb >= 8) {
            multiplier = 2;
        }
        if (multiplier > 1) {
            log.info("Set multiplier=" + multiplier + " base on size=" + sizeInGb + " gb.");
        }
        return multiplier;
    }

    public static double getTableSizeInGb(Configuration configuration, Table table) {
        double totalSize = 0.;
        if (CollectionUtils.isNotEmpty(table.getExtracts())) {
            for (Extract extract: table.getExtracts()) {
                String path = extract.getPath();
                double extractSize = getHdfsPathSizeInGb(configuration, path);
                totalSize += (extractSize / GB);
            }
        }
        return totalSize;
    }

    public static double getHdfsPathSizeInGb(Configuration configuration, String path) {
        if (StringUtils.isNotBlank(path)) {
            if (!path.endsWith(".parquet") && !path.endsWith(".avro")) {
                // default is avro
                path = PathUtils.toAvroGlob(path);
            }
            RetryTemplate retry = RetryUtils.getRetryTemplate(3);
            final String globPath = path;
            long extractSize = 0;
            try {
                extractSize = retry.execute(ctx -> HdfsUtils.getTotalBytes(configuration, globPath));
            } catch (IOException e) {
                log.warn("Failed to get extract size for " + path);
            }
            return extractSize / GB;
        } else {
            return 0.0;
        }
    }

    /**
     * 16 -> 8G
     * 24 -> 12G
     * 32 -> 16G
     * 40 -> 20G
     * 48 -> 24G
     */
    public static int scaleDataFlowAmMemGbByNumModels(int numModels) {
        int div8 = (int) Math.ceil(numModels * 1.0D / 8.0D);
        return Math.min(Math.max(4, div8 * 4), 24); // between 4G and 24G
    }

    /**
     * 16 -> 2
     * 24 -> 3
     * 32 -> 4
     * 40 -> 5
     * 48 -> 6
     */
    public static int scaleDataFlowAmVCoresByNumModels(int numModels) {
        int div8 = (int) Math.ceil(numModels * 1.0D / 8.0D);
        return Math.min(Math.max(1, div8), 6); // between 1 and 6
    }

}
