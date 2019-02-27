package com.latticeengines.monitor.util;

import java.net.InetAddress;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MISC utility functions for monitoring
 */
public class MonitoringUtils {
    private static final Logger log = LoggerFactory.getLogger(MonitoringUtils.class);

    private static final String UNKNOWN_HOSTNAME = "unknown";
    private static final String METRIC_ADVERTISE_NAME = "METRIC_ADVERTISE_NAME";

    /**
     * Try to resolve current hostname from multiple sources. An predefined unknown constant will be returned if
     * hostname cannot be resolved.
     *
     * @return non null hostname
     */
    public static String getHostName() {
        try {
            // try env first
            String advertiseName = System.getenv(METRIC_ADVERTISE_NAME);
            // use system host name
            if (StringUtils.isBlank(advertiseName)) {
                advertiseName = InetAddress.getLocalHost().getHostName();
            }
            return StringUtils.isBlank(advertiseName) ? UNKNOWN_HOSTNAME : advertiseName;
        } catch (Exception e) {
            log.error("Hostname can not be resolved", e);
            return UNKNOWN_HOSTNAME;
        }
    }
}
