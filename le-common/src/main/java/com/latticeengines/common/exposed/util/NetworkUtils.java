package com.latticeengines.common.exposed.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class NetworkUtils {

    private static final Log log = LogFactory.getLog(NetworkUtils.class);

    public static String getHostName() {
        String hostname = null;
        try {
            InetAddress addr;
            addr = InetAddress.getLocalHost();
            hostname = addr.getHostName();
        } catch (UnknownHostException ex) {
            log.error("Hostname can not be resolved");
        }

        return hostname;
    }

}
