package com.latticeengines.marketoadapter;

import org.springframework.context.annotation.Configuration;

@Configuration
public class MarketoAdapterProperties {
    public String getPod() {
        return getProperty("marketoadapter.pod");
    }

    public String getZooKeeperAddress() {
        return getProperty("marketoadapter.zookeeper.address");
    }

    public String getSkaldAddress() {
        return getProperty("marketoadapter.skald.address");
    }

    private String getProperty(String name) {
        String result = System.getProperty(name);
        if (result == null) {
            throw new RuntimeException("No value specified for property " + name);
        }

        return result;
    }
}
