package com.latticeengines.common.exposed.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkUtils {

    private static final Logger log = LoggerFactory.getLogger(NetworkUtils.class);

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
